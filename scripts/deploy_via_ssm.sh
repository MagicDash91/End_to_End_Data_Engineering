#!/usr/bin/env bash
# Ship the current commit to the EC2 host: upload a git-archive bundle to S3, then run de-deploy over SSM
# and stream its result. Used by .github/workflows/deploy.yml; also works by hand with AWS credentials.
#   scripts/deploy_via_ssm.sh <artifacts-bucket> <instance-id> [git-ref]
set -euo pipefail
bucket="$1"
instance_id="$2"
ref="${3:-HEAD}"
sha=$(git rev-parse "$ref")

bundle=$(mktemp -t release-XXXXXX.tar.gz)
git archive --format=tar.gz -o "$bundle" "$sha"
aws s3 cp "$bundle" "s3://$bucket/releases/sha/$sha.tar.gz"
aws s3 cp "s3://$bucket/releases/sha/$sha.tar.gz" "s3://$bucket/releases/current.tar.gz"
rm -f "$bundle"
echo "uploaded release $sha"

echo "waiting for $instance_id to register with SSM ..."
status=""
for _ in $(seq 1 60); do
  status=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$instance_id" \
           --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)
  [ "$status" = "Online" ] && break
  sleep 10
done
[ "$status" = "Online" ] || { echo "instance never came online in SSM (status: $status)"; exit 1; }

command_id=$(aws ssm send-command \
  --instance-ids "$instance_id" \
  --document-name AWS-RunShellScript \
  --comment "deploy $sha" \
  --parameters '{"commands":["cloud-init status --wait || true","/usr/local/bin/de-deploy"],"executionTimeout":["3600"]}' \
  --query Command.CommandId --output text)
echo "ssm command $command_id"

while true; do
  status=$(aws ssm get-command-invocation --command-id "$command_id" --instance-id "$instance_id" \
           --query Status --output text 2>/dev/null || echo Pending)
  case "$status" in
    Pending|InProgress|Delayed) sleep 15 ;;
    *) break ;;
  esac
done

aws ssm get-command-invocation --command-id "$command_id" --instance-id "$instance_id" \
  --query '[StandardOutputContent, StandardErrorContent]' --output text | tail -n 60
echo "deploy status: $status"
[ "$status" = "Success" ]
