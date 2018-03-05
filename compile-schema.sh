#!/bin/sh

avrotools=$(clj -Spath | sed $'s/:/\\\n/g' | grep avro-tools)
classpath=$(clj -Spath)
cd avro
[ -f example ] && rm -r example
java -jar "$avrotools" compile schema user.avsc .
javac -cp $classpath -d ./example $(ls ./example/**/*.java)
cd example
jar cvf example.jar *
