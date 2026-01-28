./collector/spot-dataset/aws/batch-test/scripts/destroy_infra.sh \
    -v "vpc-0a6d57e4cc2779652" \
    -s '["subnet-08edefc343a761f2c", "subnet-035ac23284fe21ed7", "subnet-04646c2d640a18745", "subnet-08bc19be5d9eb80c0"]' \
    -g '["sg-0c7cb9552f53f08b0"]' \
    -i "320674564649.dkr.ecr.us-west-2.amazonaws.com/spotlake-batch-test:latest"