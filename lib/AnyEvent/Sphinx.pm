package AnyEvent::Sphinx;

use 5.010001;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Sphinx::Query;
use AnyEvent::Sphinx::Results;

our $VERSION = '0.01';

# known searchd commands
use constant SEARCHD_COMMAND_SEARCH	=> 0;
use constant SEARCHD_COMMAND_EXCERPT	=> 1;
use constant SEARCHD_COMMAND_UPDATE	=> 2;
use constant SEARCHD_COMMAND_KEYWORDS	=> 3;
use constant SEARCHD_COMMAND_PERSIST	=> 4;
use constant SEARCHD_COMMAND_STATUS	=> 5;
use constant SEARCHD_COMMAND_QUERY	=> 6;
use constant SEARCHD_COMMAND_FLUSHATTRS	=> 7;

# current client-side command implementation versions
use constant VER_COMMAND_SEARCH		=> 0x117;
use constant VER_COMMAND_EXCERPT	=> 0x100;
use constant VER_COMMAND_UPDATE	        => 0x102;
use constant VER_COMMAND_KEYWORDS       => 0x100;
use constant VER_COMMAND_STATUS         => 0x100;
use constant VER_COMMAND_QUERY         => 0x100;
use constant VER_COMMAND_FLUSHATTRS    => 0x100;

# known searchd status codes
use constant SEARCHD_OK			=> 0;
use constant SEARCHD_ERROR		=> 1;
use constant SEARCHD_RETRY		=> 2;
use constant SEARCHD_WARNING		=> 3;

sub new {
}

# connect to searchd on $host:$port and send the query represented by
# the AnyEvent::Sphinx::Query object ($query).  The result will be an
# AnyEvent::Sphinx::Results object passed to the callback ($cb)

sub execute {
	my ($host, $port, $query, $cb) = @_;

	# store results here
	my ($response, $header, $body);

	my $handle; $handle = new AnyEvent::Handle
		connect  => [$host => $port],
		on_error => sub {
			$cb->("ERROR: $!");
			$handle->destroy; # explicitly destroy handle
		},
         on_eof   => sub {
			$cb->($response, $header, $body);
			$handle->destroy; # explicitly destroy handle
		};

	# read 4 byte protocol version
	$handle->push_read(chunk => 4, sub {
		my $ver = unpack("N*", $_[1]);
		print "server version: $ver\n";
	});

	# send our version
	my $client_ver = pack("N", 1);
	$handle->push_write($client_ver);

	# send the query
	my $request = $query->serialize;
	my $num_queries = 1; # TODO: allow for > 1
	my $full_request = pack ("nnN/a*",
		SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, $request); 
	$handle->push_write($full_request);

	# read the response header and response
	my $response_size = 0;
	$handle->push_read(chunk => 8, sub {
		my ($handle, $header) = @_;
		my ($status, $ver, $len) = unpack("n2N", $header);
		$response_size = $len;

		# read the response
		$handle->push_read(chunk => $response_size, sub {
			my ($handle, $response) = @_;
			# TODO: parse response (see RunQueries)
			my $results = AnyEvent::Sphinx::Results->new(
				response => \$response);
			$cb->($results);
		});
	});
	return $self;
}

# Autoload methods go after =cut, and are processed by the autosplit program.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

AnyEvent::Sphinx - Perl extension for blah blah blah

=head1 SYNOPSIS

  use AnyEvent::Sphinx;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for AnyEvent::Sphinx, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

jzawodn, E<lt>jzawodn@E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by jzawodn

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
