package AnyEvent::Sphinx::Query;
use Carp;
use Encode qw(encode_utf8 decode_utf8);

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

# known grouping functions
use constant SPH_GROUPBY_DAY      => 0;
use constant SPH_GROUPBY_WEEK     => 1;
use constant SPH_GROUPBY_MONTH    => 2;
use constant SPH_GROUPBY_YEAR     => 3;
use constant SPH_GROUPBY_ATTR     => 4;
use constant SPH_GROUPBY_ATTRPAIR => 5;

sub new {
	my ($class, %arg) = @_;
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
	};
	bless $self, $class;
	return $self;
}

# turn a query into bytes ready to send to sphinx (was AddQuery() in the
# old code)

sub serialize {
    my $self = shift;
    my $query = shift || '';
    my $index = shift || '*';
    my $comment = shift || '';

    ##################
    # build request
    ##################

    my $req;
    $req = pack ( "NNNNN", $self->{_offset}, $self->{_limit}, $self->{_mode}, $self->{_ranker}, $self->{_sort} ); # mode and limits
    $req .= pack ( "N/a*", $self->{_sortby});
    $req .= pack ( "N/a*", $self->{_string_encoder}->($query) ); # query itself
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

1;
