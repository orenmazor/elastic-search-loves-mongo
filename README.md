<pre>
            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
                    Version 2, December 2004

 Copyright (C) 2012 Oren Mazor <oren.mazor@gmail.com>

 Everyone is permitted to copy and distribute verbatim or modified
 copies of this license document, and changing it is allowed as long
 as the name is changed.

            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

  0. You just DO WHAT THE FUCK YOU WANT TO.
</pre>

<p>I wrote this tool so that I can consume an extremely high bandwidth mongodb cluster via an ElasticSearch (lucene) index cluster. I need everything to be real time (i.e. the ES index HAS to be as close as possible to the mongodb cluster), and so all of the work I do in this repository is aiming for that goal.</p>

<p>Current versions of guaranteed compatibility: ElasticSearch 0.19.2, MongoDB 2.0.4, Python 2.7.2</p>