##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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

# LaTeX2HTML support for the xmlmarkup package.  Doesn't do indexing.

package main;


sub do_cmd_element{
    local($_) = @_;
    my $name = next_argument();
    return "<tt class='element'>$name</tt>" . $_;
}

sub do_cmd_attribute{
    local($_) = @_;
    my $name = next_argument();
    return "<tt class='attribute'>$name</tt>" . $_;
}

sub do_env_attributedesc{
    local($_) = @_;
    my $name = next_argument();
    my $valuetype = next_argument();
    return ("\n<dl class='macrodesc'>"
            . "\n<dt><b><tt class='macro'>$name</tt></b>"
            . "&nbsp;&nbsp;&nbsp;($valuetype)"
            . "\n<dd>"
            . $_
            . "</dl>");
}

sub do_env_elementdesc{
    local($_) = @_;
    my $name = next_argument();
    my $contentmodel = next_argument();
    return ("\n<dl class='elementdesc'>"
            . "\n<dt class='start-tag'><tt>&lt;"
            . "<b class='element'>$name</b>&gt;</tt>"
            . "\n<dd class='content-model'>$contentmodel"
            . "\n<dt class='endtag'><tt>&lt;/"
            . "<b class='element'>$name</b>&gt;</tt>"
            . "\n<dd class='descrition'>"
            . $_
            . "</dl>");
}

1;				# Must end with this, because Perl is bogus.
