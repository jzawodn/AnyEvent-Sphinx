package AnyEvent::Sphinx::Results;
use Config;
use Math::BigInt;

# known searchd status codes
use constant SEARCHD_OK       => 0;
use constant SEARCHD_ERROR    => 1;
use constant SEARCHD_RETRY    => 2;
use constant SEARCHD_WARNING  => 3;

# known attribute types
use constant SPH_ATTR_INTEGER   => 1;
use constant SPH_ATTR_TIMESTAMP => 2;
use constant SPH_ATTR_ORDINAL   => 3;
use constant SPH_ATTR_BOOL      => 4;
use constant SPH_ATTR_FLOAT     => 5;
use constant SPH_ATTR_BIGINT    => 6;
use constant SPH_ATTR_STRING    => 7;
use constant SPH_ATTR_MULTI     => 0x40000000;

my $is_native64 = $Config{longsize} == 8
	|| defined $Config{use64bitint}
	|| defined $Config{use64bitall};

sub new {
	my ($class, %arg) = @_;
	my $self = { %arg };
	bless $self, $class;

	if ($self->{response}) {
		$self->parse_response($self->{response});
		# results go in $self->{results}
	}

	return $self;
}

# parse the blob that comes back from searchd into something more useful

sub parse_response {
	my ($self, $response) = @_;

    my $p = 0;
    my $max = length($response); # Protection from broken response

    my @results;
    for (my $ires = 0; $ires < $nreqs; $ires++) {
	my $result = {};	# Empty hash ref
	push(@results, $result);
	$result->{matches} = []; # Empty array ref
	$result->{error} = "";
	$result->{warnings} = "";

	# extract status
	my $status = unpack("N", substr ( $response, $p, 4 ) ); $p += 4;
	if ($status != SEARCHD_OK) {
	    my $len = unpack("N", substr ( $response, $p, 4 ) ); $p += 4;
	    my $message = substr ( $response, $p, $len ); $p += $len;
	    if ($status == SEARCHD_WARNING) {
		$result->{warning} = $message;
	    }
	    else {
		$result->{error} = $message;
		next;
	    }	    
	}

	# read schema
	my @fields;
	my (%attrs, @attr_list);

	my $nfields = unpack ( "N", substr ( $response, $p, 4 ) ); $p += 4;
	while ( $nfields-->0 && $p<$max ) {
	    my $len = unpack ( "N", substr ( $response, $p, 4 ) ); $p += 4;
	    push(@fields, substr ( $response, $p, $len )); $p += $len;
	}
	$result->{"fields"} = \@fields;

	my $nattrs = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	while ( $nattrs-->0 && $p<$max  ) {
	    my $len = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	    my $attr = substr ( $response, $p, $len ); $p += $len;
	    my $type = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	    $attrs{$attr} = $type;
	    push(@attr_list, $attr);
	}
	$result->{"attrs"} = \%attrs;

	# read match count
	my $count = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	my $id64 = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;

	# read matches
	while ( $count-->0 && $p<$max ) {
	    my $data = {};
	    if ($id64) {
		$data->{doc} = $self->_sphUnpackU64(substr($response, $p, 8)); $p += 8;
		$data->{weight} = unpack("N*", substr($response, $p, 4)); $p += 4;
	    }
	    else {
		( $data->{doc}, $data->{weight} ) = unpack("N*N*", substr($response,$p,8));
		$p += 8;
	    }
	    foreach my $attr (@attr_list) {
		if ($attrs{$attr} == SPH_ATTR_BIGINT) {
		    $data->{$attr} = $self->_sphUnpackI64(substr($response, $p, 8)); $p += 8;
		    next;
		}
		if ($attrs{$attr} == SPH_ATTR_FLOAT) {
		    my $uval = unpack( "N*", substr ( $response, $p, 4 ) ); $p += 4;
		    $data->{$attr} = [ unpack("f*", pack("L", $uval)) ];
		    next;
		}
		my $val = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
		if ($attrs{$attr} & SPH_ATTR_MULTI) {
		    my $nvalues = $val;
		    $data->{$attr} = [];
		    while ($nvalues-->0 && $p < $max) {
			$val = unpack( "N*", substr ( $response, $p, 4 ) ); $p += 4;
			push(@{$data->{$attr}}, $val);
		    }
		}
		if ($attrs{$attr} == SPH_ATTR_STRING) {
			$data->{$attr} = substr $response, $p, $val;
			$p += $val;
		}
		else {
		    $data->{$attr} = $val;
		}
	    }
	    push(@{$result->{matches}}, $data);
	}
	my $words;
	($result->{total}, $result->{total_found}, $result->{time}, $words) = unpack("N*N*N*N*", substr($response, $p, 16));
	$result->{time} = sprintf ( "%.3f", $result->{"time"}/1000 );
	$p += 16;

	while ( $words-->0 && $p < $max) {
	    my $len = unpack ( "N*", substr ( $response, $p, 4 ) ); 
	    $p += 4;
	    my $word = $self->{_string_decoder}->( substr ( $response, $p, $len ) ); 
	    $p += $len;
	    my ($docs, $hits) = unpack ("N*N*", substr($response, $p, 8));
	    $p += 8;
	    $result->{words}{$word} = {
		"docs" => $docs,
		"hits" => $hits
		};
	}
    }

    $self->{results} = \@results;
}

# portably unpack 64 unsigned bits, network order to numeric
sub _sphUnpackU64 {
    my $self = shift;
    my $v = shift;

    my ($h,$l) = unpack ( "N*N*", $v );

    # x64 route
    return ($h<<32) + $l if $is_native64;

    # x32 route, BigInt
    $h = Math::BigInt->new($h);
    $h->blsft(32)->badd($l);
    
    return $h->bstr;
}

# portably unpack 64 signed bits, network order to numeric
sub _sphUnpackI64 {
    my $self = shift;
    my $v = shift;

    my ($h,$l) = unpack ( "N*N*", $v );

    my $neg = ($h & 0x80000000) ? 1 : 0;

    # x64 route
    if ( $is_native64 ) {
	return -(~(($h<<32) + $l) + 1) if $neg;
	return ($h<<32) + $l;
    }

    # x32 route, BigInt
    if ($neg) {
	$h = ~$h;
	$l = ~$l;
    }

    my $x = Math::BigInt->new($h);
    $x->blsft(32)->badd($l);
    $x->binc()->bneg() if $neg;

    return $x->bstr;
}

1;
