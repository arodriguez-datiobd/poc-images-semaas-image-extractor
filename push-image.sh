#!/bin/bash

latest=`cat .latest-docker`
image_name="arodriguezdatio/seemas-image-extractor"

echo "Which version do you want to push? Latest version: " $latest
read version
version=${version:-$latest}

echo "Pushing " $image_name":"$version
docker tag $image_name $image_name/$version
docker push $image_name:$version
