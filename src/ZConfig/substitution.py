##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Substitution support for ZConfig values."""

import ZConfig


def substitute(s, mapping):
    """Interpolate variables from `mapping` into `s`."""
    if "$" in s:
        result = ''
        rest = s
        while rest:
            p, name, namecase, rest = _split(rest)
            result += p
            if name:
                v = mapping.get(name)
                if v is None:
                    raise ZConfig.SubstitutionReplacementError(s, namecase)
                result += v
        return result
    else:
        return s


def isname(s):
    """Return True iff s is a valid substitution name."""
    m = _name_match(s)
    if m:
        return m.group() == s
    else:
        return False


def _split(s):
    # Return a four tuple:  prefix, name, namecase, suffix
    # - prefix is text that can be used literally in the result (may be '')
    # - name is a referenced name, or None
    # - namecase is the name with case preserved
    # - suffix is trailling text that may contain additional references
    #   (may be '' or None)
    if "$" in s:
        i = s.find("$")
        c = s[i+1:i+2]
        if c == "":
            raise ZConfig.SubstitutionSyntaxError(
                "illegal lone '$' at end of source")
        if c == "$":
            return s[:i+1], None, None, s[i+2:]
        prefix = s[:i]
        if c == "{":
            m = _name_match(s, i + 2)
            if not m:
                raise ZConfig.SubstitutionSyntaxError(
                    "'${' not followed by name")
            name = m.group(0)
            i = m.end() + 1
            if not s.startswith("}", i - 1):
                raise ZConfig.SubstitutionSyntaxError(
                    "'${%s' not followed by '}'" % name)
        else:
            m = _name_match(s, i+1)
            if not m:
                raise ZConfig.SubstitutionSyntaxError(
                    "'$' not followed by '$' or name")
            name = m.group(0)
            i = m.end()
        return prefix, name.lower(), name, s[i:]
    else:
        return s, None, None, None


import re
_name_match = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*").match
del re
