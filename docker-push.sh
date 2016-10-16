#! /bin/bash
## 
## See http://dev.mikamai.com/post/144501132499/continuous-delivery-with-travis-and-ecs
##

# Push only if it's not a pull request
if [ -z "$TRAVIS_PULL_REQUEST" ] || [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  # Push only if we're testing the master branch
  if [ "$TRAVIS_BRANCH" == "master"]; then
  
    # This is needed to login on AWS and push the image on ECR
    # Change it accordingly to your docker repo
    pip install --user awscli
    export PATH=$PATH:$HOME/.local/bin
    eval $(aws ecr get-login --region $AWS_DEFAULT_REGION)
    
    # Tag and push
    echo "Pushing $IMAGE_NAME:latest"
    docker tag $IMAGE_NAME:latest "$REMOTE_IMAGE_URL:latest"
    docker push "$REMOTE_IMAGE_URL:latest"
    echo "Pushed $IMAGE_NAME:latest"
  else
    echo "Skipping deploy because branch is not 'master'"
  fi
else
  echo "Skipping deploy because it's a pull request"
fi
