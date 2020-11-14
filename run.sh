command=$@
if [ -z "$command" ]
then
      echo "\$command is empty"
else
      echo "Building with args $command"
      docker build -t audesome . && docker run -it audesome:latest sbt run $command
fi
