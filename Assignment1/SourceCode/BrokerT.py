
# Make sure you run this file at the first step for every unit tests.

from Broker import Broker

def main():
	xsub_port = '5556'
	xpub_port = '5557'
	broker = Broker(xsub_port, xpub_port)
	broker.handler()
	while True:
		pass
if __name__ == '__main__':
    main()
