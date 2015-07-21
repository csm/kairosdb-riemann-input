# Riemann Protocol input for KairosDB

This is an input for [KairosDB](http://kairosdb.github.io) that accepts metrics formatted using [Riemann's](http://riemann.io) Protocol Buffers format.

The general idea is you can add this to your `kairosdb.properties`:

    kairosdb.riemannserver.port=5555
    kairosdb.riemannserver.address=0.0.0.0
    kairosdb.service.riemann=org.metastatic.kairosdb.riemann.RiemannModule
    
Then in your Riemann config:

    (require '[riemann.client])    
    (streams
      (forward (riemann.client/tcp-client {:host "kairosdb.foo.com" :port 5555}))

If events have attributes in them, each attribute will become a tag.

Any tag in the "tags" list will be split into two parts, separated by a colon (`:`). If there are two non-empty parts, that becomes a tag as well.

If you send queries to your KairosDB server, they will be ignored and unanswered. We could conceivably support that, but not now.