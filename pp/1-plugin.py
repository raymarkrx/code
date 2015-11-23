import re
_RE_INTERCEPTROR_ENDS_WITH = re.compile(r'^\*([^\*\?]+)$')
m=_RE_INTERCEPTROR_ENDS_WITH.match('*ad')
print m
