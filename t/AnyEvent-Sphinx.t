use Test::More tests => 2;
BEGIN { use_ok('AnyEvent::Sphinx') };

my $sphinx = AnyEvent::Sphinx->new();

# new
ok($sphinx, "new()");

exit;
