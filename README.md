# Riemann Protocol input for KairosDB

This is an input for [KairosDB](http://kairosdb.github.io) that accepts metrics formatted using [Riemann's](http://riemann.io) Protocol Buffers format.

If events have attributes in them, each attribute will become a tag.

Any tag in the "tags" list will be split into two parts, separated by a colon (`:`). If there are two non-empty parts, that becomes a tag as well.