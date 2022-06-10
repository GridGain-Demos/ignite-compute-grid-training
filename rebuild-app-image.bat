docker-compose -f docker/ignite-compute-app.yaml stop
docker container rm ignite-compute-app
docker image rm ignite-compute-app

mvn clean package
docker build -f docker/ComputeAppDockerfile -t ignite-compute-app .

