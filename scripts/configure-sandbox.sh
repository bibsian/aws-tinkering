#!/bin/bash
# Run this every time you start a new Pluralsight sandbox session.
# Sandbox credentials expire after ~4 hours.

set -e

echo "=== Pluralsight Sandbox - AWS Credential Setup ==="
echo ""
echo "How to get your Access Keys from the sandbox console:"
echo "  1. Log into the AWS Console with your sandbox username/password"
echo "  2. Click your username (top-right) > Security credentials"
echo "  3. Under 'Access keys', click 'Create access key'"
echo "  4. Copy the Access Key ID and Secret Access Key below"
echo ""

read -p "AWS Access Key ID:     " aws_access_key_id
read -s -p "AWS Secret Access Key: " aws_secret_access_key
echo ""
read -p "Default region [us-east-1]: " aws_region
aws_region=${aws_region:-us-east-1}

aws configure set aws_access_key_id "$aws_access_key_id" --profile pluralsight
aws configure set aws_secret_access_key "$aws_secret_access_key" --profile pluralsight
aws configure set region "$aws_region" --profile pluralsight
aws configure set output "json" --profile pluralsight
aws configure set verify_ssl false --profile pluralsight  # Pluralsight proxy uses self-signed cert

echo ""
echo "Credentials saved to profile 'pluralsight'."

# Check if ~/.zshrc already has the SSL workaround
if ! grep -q "no-verify-ssl" ~/.zshrc 2>/dev/null; then
  echo ""
  echo "One-time setup: adding SSL workaround to ~/.zshrc (AWS CLI v2 + Pluralsight proxy)"
  echo "alias aws='aws --no-verify-ssl'" >> ~/.zshrc
  echo "export PYTHONWARNINGS='ignore:Unverified HTTPS request'" >> ~/.zshrc
  echo "Added. Run 'source ~/.zshrc' or open a new terminal to activate."
fi

echo ""
echo "Verifying connection..."
aws sts get-caller-identity --profile pluralsight --no-verify-ssl
echo ""
echo "All set! Use --profile pluralsight (or AWS_PROFILE=pluralsight) with any AWS command."
