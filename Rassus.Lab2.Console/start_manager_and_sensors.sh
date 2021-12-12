#!/bin/bash

set -x
./pids.sh
echo "#!/bin/bash" > pids.sh
echo "set -x" >> pids.sh

dotnet bin/Debug/net6.0/Rassus.Lab2.Console.dll comander comander &
echo $id
sleep 1s
dotnet bin/Debug/net6.0/Rassus.Lab2.Console.dll peer peer &
sleep 1s
dotnet bin/Debug/net6.0/Rassus.Lab2.Console.dll peer peer

