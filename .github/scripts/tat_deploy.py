#!/usr/bin/env python3
"""Deploy 7L app to Tencent Cloud server via TAT API.

Uses TC3-HMAC-SHA256 signing to call TAT RunCommand API.
Files are base64-encoded and written to the server via shell commands.
"""

import os
import sys
import json
import time
import hmac
import hashlib
import base64
import datetime
import subprocess

# Config from environment
SECRET_ID = os.environ["TENCENT_SECRET_ID"]
SECRET_KEY = os.environ["TENCENT_SECRET_KEY"]
INSTANCE_ID = os.environ["TENCENT_INSTANCE_ID"]
REGION = os.environ.get("TENCENT_REGION", "ap-beijing")

HOST = "tat.tencentcloudapi.com"
SERVICE = "tat"
ALGORITHM = "TC3-HMAC-SHA256"
CT = "application/json; charset=utf-8"


def _hmac_sha256(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def sign_and_call(action, payload, max_retries=3):
    """Sign a TAT API request and call it."""
    body = json.dumps(payload)
    timestamp = int(time.time())
    date = datetime.datetime.fromtimestamp(timestamp, datetime.UTC).strftime("%Y-%m-%d")

    canonical_headers = f"content-type:{CT}\nhost:{HOST}\nx-tc-action:{action.lower()}\n"
    signed_headers = "content-type;host;x-tc-action"
    hashed_payload = hashlib.sha256(body.encode()).hexdigest()
    canonical_request = f"POST\n/\n\n{canonical_headers}\n{signed_headers}\n{hashed_payload}"

    credential_scope = f"{date}/{SERVICE}/tc3_request"
    string_to_sign = f"{ALGORITHM}\n{timestamp}\n{credential_scope}\n" + hashlib.sha256(
        canonical_request.encode()
    ).hexdigest()

    secret_date = _hmac_sha256(f"TC3{SECRET_KEY}".encode(), date)
    secret_service = _hmac_sha256(secret_date, SERVICE)
    secret_signing = _hmac_sha256(secret_service, "tc3_request")
    signature = hmac.new(secret_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

    authorization = (
        f"{ALGORITHM} Credential={SECRET_ID}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    curl_cmd = ["curl", "-s", "-X", "POST", f"https://{HOST}"]
    for h in [
        f"Authorization: {authorization}",
        f"Content-Type: {CT}",
        f"Host: {HOST}",
        f"X-TC-Action: {action}",
        f"X-TC-Timestamp: {timestamp}",
        f"X-TC-Version: 2020-10-28",
        f"X-TC-Region: {REGION}",
    ]:
        curl_cmd.extend(["-H", h])
    curl_cmd.extend(["-d", body])

    for attempt in range(max_retries):
        try:
            resp = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
            result = json.loads(resp.stdout)
            if "Response" in result:
                return result["Response"]
            print(f"Unexpected response (attempt {attempt+1}): {str(result)[:200]}")
        except Exception as e:
            print(f"Request failed (attempt {attempt+1}): {e}")
        time.sleep(2 ** attempt)

    return None


def run_command(cmd, timeout=120, wait=15):
    """Run a command on the server via TAT and return the output."""
    payload = {
        "InstanceIds": [INSTANCE_ID],
        "CommandType": "SHELL",
        "Content": base64.b64encode(cmd.encode()).decode(),
        "Timeout": timeout,
    }

    resp = sign_and_call("RunCommand", payload)
    if not resp or "InvocationId" not in resp:
        print(f"Failed to run command: {resp}")
        return None

    inv_id = resp["InvocationId"]
    print(f"Command submitted: {inv_id}")

    # Wait for completion
    time.sleep(wait)

    # Get invocation details
    inv_resp = sign_and_call("DescribeInvocations", {"InvocationIds": [inv_id]})
    if not inv_resp or "InvocationSet" not in inv_resp:
        print(f"Failed to get invocation: {inv_resp}")
        return None

    inv_set = inv_resp["InvocationSet"]
    if not inv_set:
        print("No invocation found")
        return None

    task_ids = [t["InvocationTaskId"] for t in inv_set[0].get("InvocationTaskBasicInfoSet", [])]
    if not task_ids:
        print("No tasks found")
        return None

    # Get task output
    task_resp = sign_and_call("DescribeInvocationTasks", {"InvocationTaskIds": task_ids})
    if not task_resp or "InvocationTaskSet" not in task_resp:
        print(f"Failed to get tasks: {task_resp}")
        return None

    for t in task_resp["InvocationTaskSet"]:
        output_b64 = t.get("TaskResult", {}).get("Output", "")
        status = t.get("TaskStatus", "UNKNOWN")
        if output_b64:
            return base64.b64decode(output_b64).decode(errors="replace")
        else:
            print(f"Task status: {status}")

    return None


def deploy_file(local_path, remote_path, chunk_size=30000):
    """Deploy a file to the server by splitting into base64 chunks."""
    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()

    chunks = [content_b64[i : i + chunk_size] for i in range(0, len(content_b64), chunk_size)]
    print(f"Deploying {local_path} -> {remote_path} ({len(chunks)} chunks)")

    # Write first chunk
    cmd = f"echo '{chunks[0]}' > /tmp/deploy_b64"
    run_command(cmd, wait=5)

    # Append remaining chunks
    for chunk in chunks[1:]:
        cmd = f"echo '{chunk}' >> /tmp/deploy_b64"
        run_command(cmd, wait=5)

    # Decode and move to target
    cmd = f"base64 -d /tmp/deploy_b64 > {remote_path} && rm /tmp/deploy_b64 && echo 'OK: {remote_path}'"
    result = run_command(cmd, wait=8)
    print(f"Result: {result}")
    return result


def main():
    """Main deployment flow."""
    workspace = os.environ.get("GITHUB_WORKSPACE", ".")

    # 1. Deploy frontend files
    frontend_dir = f"{workspace}/frontend"
    for f in ["index.html", "style.css", "app.js"]:
        local = f"{frontend_dir}/{f}"
        if os.path.exists(local):
            # Deploy to host bind mount path
            deploy_file(local, f"/root/7l-deploy/7l-webapp/{f}")

    # 2. Deploy nginx config
    nginx_conf = f"{workspace}/nginx/default.conf"
    if os.path.exists(nginx_conf):
        deploy_file(nginx_conf, "/root/7l-deploy/nginx/default.conf")

    # 3. Reload nginx
    print("Reloading nginx...")
    result = run_command("docker exec 7l-nginx nginx -s reload && echo 'nginx reloaded'", wait=8)
    print(f"Nginx reload: {result}")

    # 4. Verify
    print("Verifying deployment...")
    result = run_command(
        "curl -s http://localhost/api/v1/health && echo '' && ls -la /root/7l-deploy/7l-webapp/",
        wait=8,
    )
    print(f"Verification: {result}")

    print("Deployment complete!")


if __name__ == "__main__":
    main()
