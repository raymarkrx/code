

from pkg_resources import iter_entry_points

a= dict((ep.name, ep) for ep in iter_entry_points('apscheduler.triggers'))

