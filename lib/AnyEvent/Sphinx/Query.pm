package AnyEvent::Sphinx::Query;
use strict;
use warnings;
use Carp;
use Config;
use Encode qw(encode_utf8);
use base 'Exporter';

my $is_native64 = $Config{longsize} == 8 || defined $Config{use64bitint} || defined $Config{use64bitall};

# known match modes
use constant SPH_MATCH_ALL       => 0;
use constant SPH_MATCH_ANY       => 1;
use constant SPH_MATCH_PHRASE    => 2;
use constant SPH_MATCH_BOOLEAN   => 3;
use constant SPH_MATCH_EXTENDED  => 4;
use constant SPH_MATCH_FULLSCAN  => 5;
use constant SPH_MATCH_EXTENDED2 => 6;

# known ranking modes (ext2 only)
use constant SPH_RANK_PROXIMITY_BM25 => 0;
use constant SPH_RANK_BM25           => 1;
use constant SPH_RANK_NONE           => 2;
use constant SPH_RANK_WORDCOUNT      => 3;
use constant SPH_RANK_PROXIMITY      => 4;
use constant SPH_RANK_MATCHANY       => 5;

# known sort modes
use constant SPH_SORT_RELEVANCE     => 0;
use constant SPH_SORT_ATTR_DESC     => 1;
use constant SPH_SORT_ATTR_ASC      => 2;
use constant SPH_SORT_TIME_SEGMENTS => 3;
use constant SPH_SORT_EXTENDED	    => 4;
use constant SPH_SORT_EXPR	        => 5;

# known filter types
use constant SPH_FILTER_VALUES      => 0;
use constant SPH_FILTER_RANGE       => 1;
use constant SPH_FILTER_FLOATRANGE  => 2;

# known attribute types
use constant SPH_ATTR_INTEGER		=> 1;
use constant SPH_ATTR_TIMESTAMP		=> 2;
use constant SPH_ATTR_ORDINAL		=> 3;
use constant SPH_ATTR_BOOL		=> 4;
use constant SPH_ATTR_FLOAT		=> 5;
use constant SPH_ATTR_BIGINT		=> 6;
use constant SPH_ATTR_STRING		=> 7;
use constant SPH_ATTR_MULTI		=> 0x40000000;

# known grouping functions
use constant SPH_GROUPBY_DAY      => 0;
use constant SPH_GROUPBY_WEEK     => 1;
use constant SPH_GROUPBY_MONTH    => 2;
use constant SPH_GROUPBY_YEAR     => 3;
use constant SPH_GROUPBY_ATTR     => 4;
use constant SPH_GROUPBY_ATTRPAIR => 5;

our @EXPORT = qw(	
	SPH_MATCH_ALL SPH_MATCH_ANY SPH_MATCH_PHRASE SPH_MATCH_BOOLEAN SPH_MATCH_EXTENDED
	SPH_MATCH_FULLSCAN SPH_MATCH_EXTENDED2
	SPH_RANK_PROXIMITY_BM25 SPH_RANK_BM25 SPH_RANK_NONE SPH_RANK_WORDCOUNT
	SPH_SORT_RELEVANCE SPH_SORT_ATTR_DESC SPH_SORT_ATTR_ASC SPH_SORT_TIME_SEGMENTS
	SPH_SORT_EXTENDED SPH_SORT_EXPR
	SPH_GROUPBY_DAY SPH_GROUPBY_WEEK SPH_GROUPBY_MONTH SPH_GROUPBY_YEAR SPH_GROUPBY_ATTR
	SPH_GROUPBY_ATTRPAIR
);

# Floating point number matching expression
my $num_re = qr/^-?\d*\.?\d*(?:[eE][+-]?\d+)?$/;

sub new {
	my ($class, $query, $index) = @_;
	my $self = {
		_offset		=> 0,
		_limit		=> 20,
		_mode		=> SPH_MATCH_ALL,
		_weights	=> [],
		_sort		=> SPH_SORT_RELEVANCE,
		_sortby		=> "",
		_min_id		=> 0,
		_max_id		=> 0,
		_filters	=> [],
		_groupby	=> "",
		_groupdistinct	=> "",
		_groupfunc	=> SPH_GROUPBY_DAY,
		_groupsort      => '@group desc',
		_maxmatches	=> 1000,
		_cutoff         => 0,
		_retrycount     => 0,
		_retrydelay     => 0,
		_anchor         => undef,
		_indexweights   => undef,
		_ranker         => SPH_RANK_PROXIMITY_BM25,
		_maxquerytime   => 0,
		_fieldweights   => {},
		_overrides      => {},
		_select         => q{*},
		_query          => $query || undef,
		_index          => $index || undef,
	};
	bless $self, $class;
	return $self;
}

sub Query {
	my $self = shift;
	my $query = shift;
	my $index = shift || '*';
	if (defined $query) {
		$self->{_query} = $query;
	}
	return $self;
}

sub Index {
	my $self = shift;
	my $index = shift;
	if (defined $index) {
		$self->{_index} = $index;
	}
	return $self;
}

# turn a query into bytes ready to send to sphinx (was AddQuery() in the
# old code)

sub serialize {
    my $self = shift;
    my $query = $self->{_query};
    my $index = $self->{_index};
    my $comment = shift || '';

    ##################
    # build request
    ##################

    my $req;
    $req = pack ( "NNNNN", $self->{_offset}, $self->{_limit}, $self->{_mode}, $self->{_ranker}, $self->{_sort} ); # mode and limits
    $req .= pack ( "N/a*", $self->{_sortby});
    $req .= pack ( "N/a*", encode_utf8($query) ); # query itself
    $req .= pack ( "N*", scalar(@{$self->{_weights}}), @{$self->{_weights}});
    $req .= pack ( "N/a*", $index); # indexes
    $req .= pack ( "N", 1) 
	. $self->_sphPackU64($self->{_min_id})
	. $self->_sphPackU64($self->{_max_id}); # id64 range

    # filters
    $req .= pack ( "N", scalar @{$self->{_filters}} );
    foreach my $filter (@{$self->{_filters}}) {
	$req .= pack ( "N/a*", $filter->{attr});
	$req .= pack ( "N", $filter->{type});

	my $t = $filter->{type};
	if ($t == SPH_FILTER_VALUES) {
	    $req .= $self->_sphPackI64array($filter->{values});
	}
	elsif ($t == SPH_FILTER_RANGE) {
	    $req .= $self->_sphPackI64($filter->{min}) . $self->_sphPackI64($filter->{max});
	}
	elsif ($t == SPH_FILTER_FLOATRANGE) {
	    $req .= _PackFloat ( $filter->{"min"} ) . _PackFloat ( $filter->{"max"} );
	}
	else {
	    croak("Unhandled filter type $t");
	}
	$req .= pack ( "N",  $filter->{exclude});
    }

    # group-by clause, max-matches count, group-sort clause, cutoff count
    $req .= pack ( "NN/a*", $self->{_groupfunc}, $self->{_groupby} );
    $req .= pack ( "N", $self->{_maxmatches} );
    $req .= pack ( "N/a*", $self->{_groupsort});
    $req .= pack ( "NNN", $self->{_cutoff}, $self->{_retrycount}, $self->{_retrydelay} );
    $req .= pack ( "N/a*", $self->{_groupdistinct});

    if (!defined $self->{_anchor}) {
	$req .= pack ( "N", 0);
    }
    else {
	my $a = $self->{_anchor};
	$req .= pack ( "N", 1);
	$req .= pack ( "N/a*", $a->{attrlat});
	$req .= pack ( "N/a*", $a->{attrlong});
	$req .= _PackFloat($a->{lat}) . _PackFloat($a->{long});
    }

    # per-index weights
    $req .= pack( "N", scalar keys %{$self->{_indexweights}});
    $req .= pack ( "N/a*N", $_, $self->{_indexweights}->{$_} ) for keys %{$self->{_indexweights}};

    # max query time
    $req .= pack ( "N", $self->{_maxquerytime} );

    # per-field weights
    $req .= pack ( "N", scalar keys %{$self->{_fieldweights}} );
    $req .= pack ( "N/a*N", $_, $self->{_fieldweights}->{$_}) for keys %{$self->{_fieldweights}};
    # comment
    $req .= pack ( "N/a*", $comment);

    # attribute overrides
    $req .= pack ( "N", scalar keys %{$self->{_overrides}} );
    for my $entry (values %{$self->{_overrides}}) {
	$req .= pack ("N/a*", $entry->{attr})
	    . pack ("NN", $entry->{type}, scalar keys %{$entry->{values}});
	for my $id (keys %{$entry->{values}}) {
	    croak "Attribute value key is not numeric" unless $id =~ m/$num_re/;
	    my $v = $entry->{values}->{$id};
	    croak "Attribute value key is not numeric" unless $v =~ m/$num_re/;
	    $req .= $self->_sphPackU64($id);
	    if ($entry->{type} == SPH_ATTR_FLOAT) {
		$req .= $self->_packfloat($v);
	    }
	    elsif ($entry->{type} == SPH_ATTR_BIGINT) {
		$req .= $self->_sphPackI64($v);
	    }
	    else {
		$req .= pack("N", $v);
	    }
	}
    }
    
    # select list
    $req .= pack("N/a*", $self->{_select} || '');
    return $req;
}

# portably pack numeric to 64 signed bits, network order
sub _sphPackI64 {
    my $self = shift;
    my $v = shift;

    # x64 route
	no warnings;
    my $i = $is_native64 ? int($v) : Math::BigInt->new("$v");
    return pack ( "NN", $i>>32, $i & 4294967295 );
}

# portably pack numeric to 64 unsigned bits, network order
sub _sphPackU64 {
    my $self = shift;
    my $v = shift;

    my $i = $is_native64 ? int($v) : Math::BigInt->new("$v");
    return pack ( "NN", $i>>32, $i & 4294967295 );
}

sub _sphPackI64array {
    my $self = shift;
    my $values = shift || [];

    my $s = pack("N", scalar @$values);
    $s .= $self->_sphPackI64($_) for @$values;
    return $s;
}

sub GetLastError {
	my $self = shift;
	return $self->{_error};
}

sub _Warning {
    my ($self, $msg) = @_;

    $self->{_warning} = $msg;
    $self->{_log}->warn($msg) if $self->{_log};
}

sub GetLastWarning {
	my $self = shift;
	return $self->{_warning};
}

sub SetServer {
    my $self = shift;
    my $host = shift;
    my $port = shift;

    croak("host is not defined") unless defined($host);
    $self->{_path} = $host, return if substr($host, 0, 1) eq '/';
    $self->{_path} = substr($host, 7), return if substr($host, 0, 7) eq 'unix://';
	
    croak("port is not an integer") unless defined($port) && $port =~ m/^\d+$/o;

    $self->{_host} = $host;
    $self->{_port} = $port;
    $self->{_path} = undef;

    return $self;
}

sub SetConnectTimeout {
    my $self = shift;
    my $timeout = shift;

    croak("timeout ($timeout) is not numeric") unless ($timeout =~  m/$num_re/);
    $self->{_timeout} = $timeout;
}

sub SetLimits {
    my $self = shift;
    my $offset = shift;
    my $limit = shift;
    my $max = shift || 0;
    croak("offset should be an integer >= 0") unless ($offset =~ /^\d+$/ && $offset >= 0) ;
    croak("limit should be an integer >= 0") unless ($limit =~ /^\d+$/ && $limit >= 0);
    $self->{_offset} = $offset;
    $self->{_limit}  = $limit;
    if($max > 0) {
	$self->{_maxmatches} = $max;
    }
    return $self;
}

sub SetMaxQueryTime {
    my $self = shift;
    my $max = shift;

    croak("max value should be an integer >= 0") unless ($max =~ /^\d+$/ && $max >= 0) ;
    $self->{_maxquerytime} = $max;
    return $self;
}

sub SetMatchMode {
        my $self = shift;
        my $mode = shift;
        croak("Match mode not defined") unless defined($mode);
        croak("Unknown matchmode: $mode") unless ( $mode==SPH_MATCH_ALL 
						   || $mode==SPH_MATCH_ANY 
						   || $mode==SPH_MATCH_PHRASE 
						   || $mode==SPH_MATCH_BOOLEAN 
						   || $mode==SPH_MATCH_EXTENDED 
						   || $mode==SPH_MATCH_FULLSCAN 
						   || $mode==SPH_MATCH_EXTENDED2 );
        $self->{_mode} = $mode;
	return $self;
}

sub SetRankingMode {
    my $self = shift;
    my $ranker = shift;

    croak("Unknown ranking mode: $ranker") unless ( $ranker==SPH_RANK_PROXIMITY_BM25
						    || $ranker==SPH_RANK_BM25
						    || $ranker==SPH_RANK_NONE
						    || $ranker==SPH_RANK_WORDCOUNT
						    || $ranker==SPH_RANK_PROXIMITY );

    $self->{_ranker} = $ranker;
    return $self;
}

sub SetSortMode {
        my $self = shift;
        my $mode = shift;
	my $sortby = shift || "";
        croak("Sort mode not defined") unless defined($mode);
        croak("Unknown sort mode: $mode") unless ( $mode == SPH_SORT_RELEVANCE
						   || $mode == SPH_SORT_ATTR_DESC
						   || $mode == SPH_SORT_ATTR_ASC 
						   || $mode == SPH_SORT_TIME_SEGMENTS
						   || $mode == SPH_SORT_EXTENDED
						   || $mode == SPH_SORT_EXPR
						   );
	croak("Sortby must be defined") unless ($mode==SPH_SORT_RELEVANCE || length($sortby));
        $self->{_sort} = $mode;
	$self->{_sortby} = $sortby;
	return $self;
}

sub SetWeights {
        my $self = shift;
        my $weights = shift;
        croak("Weights is not an array reference") unless (ref($weights) eq 'ARRAY');
        foreach my $weight (@$weights) {
                croak("Weight: $weight is not an integer") unless ($weight =~ /^\d+$/);
        }
        $self->{_weights} = $weights;
	return $self;
}

sub SetFieldWeights {
        my $self = shift;
        my $weights = shift;
        croak("Weights is not a hash reference") unless (ref($weights) eq 'HASH');
        foreach my $field (keys %$weights) {
	    croak("Weight: $weights->{$field} is not an integer >= 0") unless ($weights->{$field} =~ /^\d+$/);
        }
        $self->{_fieldweights} = $weights;
	return $self;
}

sub SetIndexWeights {
        my $self = shift;
        my $weights = shift;
        croak("Weights is not a hash reference") unless (ref($weights) eq 'HASH');
        foreach (keys %$weights) {
                croak("IndexWeight $_: $weights->{$_} is not an integer") unless ($weights->{$_} =~ /^\d+$/);
        }
        $self->{_indexweights} = $weights;
	return $self;
}

sub SetIDRange {
	my $self = shift;
	my $min = shift;
	my $max = shift;
	croak("min_id is not numeric") unless ($min =~  m/$num_re/);
	croak("max_id is not numeric") unless ($max =~  m/$num_re/);
	croak("min_id is larger than or equal to max_id") unless ($min < $max);
	$self->{_min_id} = $min;
	$self->{_max_id} = $max;
	return $self;
}

sub SetFilter {
    my ($self, $attribute, $values, $exclude) = @_;

    croak("attribute is not defined") unless (defined $attribute);
    croak("values is not an array reference") unless (ref($values) eq 'ARRAY');
    croak("values reference is empty") unless (scalar(@$values));

	no warnings;
    foreach my $value (@$values) {
	croak("value $value is not numeric") unless ($value =~ m/$num_re/);
    }
    push(@{$self->{_filters}}, {
	type => SPH_FILTER_VALUES,
	attr => $attribute,
	values => $values,
	exclude => $exclude ? 1 : 0,
    });

    return $self;
}

sub SetFilterRange {
    my ($self, $attribute, $min, $max, $exclude) = @_;
    croak("SetFilterRange: attribute is not defined") unless (defined $attribute);
    croak("SetFilterRange: min ($min) is not an integer") unless ($min =~ m/$num_re/);
    croak("SetFilterRange: max ($max) is not an integer") unless ($max =~ m/$num_re/);
    croak("SetFilterRange: min ($min) value should be <= max ($max)") unless ($min <= $max);

    push(@{$self->{_filters}}, {
	type => SPH_FILTER_RANGE,
	attr => $attribute,
	min => $min,
	max => $max,
	exclude => $exclude ? 1 : 0,
    });

    return $self;
}

sub SetFilterFloatRange {
    my ($self, $attribute, $min, $max, $exclude) = @_;
    croak("SetFilterFloatRange: attribute is not defined") unless (defined $attribute);
    croak("SetFilterFloatRange: min ($min) is not numeric") unless ($min =~ m/$num_re/);
    croak("SetFilterFloatRange: max ($max) is not numeric") unless ($max =~ m/$num_re/);
    croak("SetFilterFloatRange: min ($min) value should be <= max ($max)") unless ($min <= $max);

    push(@{$self->{_filters}}, {
	type => SPH_FILTER_FLOATRANGE,
	attr => $attribute,
	min => $min,
	max => $max,
	exclude => $exclude ? 1 : 0,
    });

    return $self;

}

sub SetGeoAnchor {
    my ($self, $attrlat, $attrlong, $lat, $long) = @_;

    croak("attrlat is not defined") unless defined $attrlat;
    croak("attrlong is not defined") unless defined $attrlong;
    croak("lat: $lat is not numeric") unless ($lat =~ m/$num_re/);
    croak("long: $long is not numeric") unless ($long =~ m/$num_re/);

    $self->{_anchor} = { 
			 attrlat => $attrlat, 
			 attrlong => $attrlong, 
			 lat => $lat,
			 long => $long,
		     };
    return $self;
}

sub SetGroupBy {
	my $self = shift;
	my $attribute = shift;
	my $func = shift;
	my $groupsort = shift || '@group desc';
	croak("attribute is not defined") unless (defined $attribute);
	croak("Unknown grouping function: $func") unless ($func==SPH_GROUPBY_DAY
							  || $func==SPH_GROUPBY_WEEK
							  || $func==SPH_GROUPBY_MONTH
							  || $func==SPH_GROUPBY_YEAR
							  || $func==SPH_GROUPBY_ATTR
							  || $func==SPH_GROUPBY_ATTRPAIR
							  );

	$self->{_groupby} = $attribute;
	$self->{_groupfunc} = $func;
	$self->{_groupsort} = $groupsort;
	return $self;
}

sub SetGroupDistinct {
    my $self = shift;
    my $attribute = shift;
    croak("attribute is not defined") unless (defined $attribute);
    $self->{_groupdistinct} = $attribute;
    return $self;
}

sub SetRetries {
    my $self = shift;
    my $count = shift;
    my $delay = shift || 0;

    croak("count: $count is not an integer >= 0") unless ($count =~ /^\d+$/o && $count >= 0);
    croak("delay: $delay is not an integer >= 0") unless ($delay =~ /^\d+$/o && $delay >= 0);
    $self->{_retrycount} = $count;
    $self->{_retrydelay} = $delay;
    return $self;
}

sub SetOverride {
    my $self = shift;
    my $attrname = shift;
    my $attrtype = shift;
    my $values = shift;

    croak("attribute name is not defined") unless defined $attrname;
    croak("Uknown attribute type: $attrtype") unless ($attrtype == SPH_ATTR_INTEGER
						      || $attrtype == SPH_ATTR_TIMESTAMP
						      || $attrtype == SPH_ATTR_BOOL
						      || $attrtype == SPH_ATTR_FLOAT
						      || $attrtype == SPH_ATTR_BIGINT);
    $self->{_overrides}->{$attrname} = { attr => $attrname,
					 type => $attrtype,
					 values => $values,
				     };
    
    return $self;
}

sub SetSelect {
    my $self = shift;
    $self->{_select} = shift;
    return $self;
}

sub ResetFilters {
    my $self = shift;

    $self->{_filters} = [];
    $self->{_anchor} = undef;

    return $self;
}

sub ResetGroupBy {
    my $self = shift;

    $self->{_groupby} = "";
    $self->{_groupfunc} = SPH_GROUPBY_DAY;
    $self->{_groupsort} = '@group desc';
    $self->{_groupdistinct} = "";

    return $self;
}

sub ResetOverrides {
    my $self = shift;

    $self->{_select} = undef;
    return $self;
}

sub _PackFloat {
    my $f = shift;
    my $t1 = pack ( "f", $f ); # machine order
    my $t2 = unpack ( "L*", $t1 ); # int in machine order
    return pack ( "N", $t2 );
}

sub EscapeString {
    my $self = shift;
    return quotemeta(shift);
}

# TODO: UpdateAttributes()
# TODO: BuildKeywords()
# TODO: BuildExcerpts()
# TODO: FlushAttrs()
# TODO: Status()

1;
