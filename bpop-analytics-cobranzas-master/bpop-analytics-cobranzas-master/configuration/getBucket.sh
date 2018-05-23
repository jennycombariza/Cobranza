REGION=$(curl 169.254.169.254/latest/meta-data/placement/availability-zone/ | sed 's/[a-z]$//')
INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
ENV=$(aws ec2 describe-tags --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=Environment" --region ${REGION} | jq '.Tags[] | .Value' | sed 's/"//g')
echo $ENV
