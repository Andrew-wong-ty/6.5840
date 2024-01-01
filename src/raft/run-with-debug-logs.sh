# reference: https://blog.josejg.com/debugging-pretty/
VERBOSE=1  go test -run TestInitialElection2A  > output.log
/usr/bin/python3 dslogs.py -c 3  output.log > out.log