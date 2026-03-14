#!/bin/bash
# Run this every time you start a new Pluralsight sandbox session.
# Sandbox credentials expire after ~4 hours.

set -e

PROFILE="pluralsight"

echo "=== Pluralsight Sandbox - AWS Credential Setup ==="
echo ""
echo "How to get your Access Keys from the sandbox console:"
echo "  1. Log into the AWS Console with your sandbox username/password (use incognito)"
echo "  2. Click your username (top-right) > Security credentials"
echo "  3. Under 'Access keys' > 'Create access key'"
echo "  4. Copy the Access Key ID and Secret Access Key below"
echo ""

read -p "AWS Access Key ID:     " aws_access_key_id
read -s -p "AWS Secret Access Key: " aws_secret_access_key
echo ""
read -p "Default region [us-east-1]: " aws_region
aws_region=${aws_region:-us-east-1}

aws configure set aws_access_key_id "$aws_access_key_id" --profile "$PROFILE"
aws configure set aws_secret_access_key "$aws_secret_access_key" --profile "$PROFILE"
aws configure set region "$aws_region" --profile "$PROFILE"
aws configure set output "json" --profile "$PROFILE"

echo ""
echo "Credentials saved to profile '$PROFILE'."
echo ""
echo "Verifying connection..."
aws sts get-caller-identity --profile "$PROFILE"
echo ""
echo "All set! Use --profile $PROFILE (or export AWS_PROFILE=$PROFILE) with any AWS command."
