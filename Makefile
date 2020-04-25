all: Proxy.class Server.class ServerI.class Cache.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
