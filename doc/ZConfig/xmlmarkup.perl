# LaTeX2HTML support for the xmlmarkup package.  Doesn't do indexing.

package main;


# sub ltx_next_argument{
#     my $param;
#     $param = missing_braces()
#       unless ((s/$next_pair_pr_rx/$param=$2;''/eo)
# 	      ||(s/$next_pair_rx/$param=$2;''/eo));
#     return $param;
# }


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
            . "&nbsp;&nbsp;&nbsp;(<tt>$valuetype</tt>)"
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
