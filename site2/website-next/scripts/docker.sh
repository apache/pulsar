
#! /bin/sh

ROOT_DIR=$(git rev-parse --show-toplevel)
CONTAINER_NAME="website-next"
CONTAINER_ID=$(docker ps | grep $CONTAINER_NAME | awk '{print $1}')

if [ -n "$CONTAINER_ID" ]
then
    docker exec -it $CONTAINER_NAME nginx -s reload
else
    docker run --name $CONTAINER_NAME -d -p 80:80 -v $ROOT_DIR/site2/website-next/build:/usr/share/nginx/html nginx
fi


echo "Website is running: http://localhost"
