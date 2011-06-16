use Test::More tests => 17;
BEGIN { use_ok('AnyEvent::Sphinx::Query') };

# new()
my $query = AnyEvent::Sphinx::Query->new();
ok($query, "new()");

# new() with query
$query = AnyEvent::Sphinx::Query->new('foo bar');
ok($query, "new('foo bar')");
is($query->{_query}, 'foo bar', "internal query string");

# Query()
ok($query->Query("gobble"), "Query()");
is($query->{_query}, 'gobble', "internal query string");

# Index()
ok($query->Index("index_1"), "Index()");
is($query->{_index}, 'index_1', "internal index string");

# SetLimits()
ok($query->SetLimits(0, 1000, 5000), "SetLimits()");
is($query->{_offset}, 0, "internal offset");
is($query->{_limit}, 1000, "internal limit");
is($query->{_maxmatches}, 5000, "internal maxmatches");

# serialize()
ok($query->serialize, "serialize()");
# TODO: compare to a known good copy?

# SetFilterRange()
ok($query->SetFilterRange('foo', 0, 100), "SetFilterRange()");
is($query->{_filters}->[0]->{attr}, 'foo', "internal filter attr");
is($query->{_filters}->[0]->{min}, 0, "internal filter min");
is($query->{_filters}->[0]->{max}, 100, "internal filter max");

exit;
