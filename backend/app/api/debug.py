# backend/app/api/debug.py
import subprocess
import json
from fastapi import APIRouter

router = APIRouter()

@router.get("/debug/opensky")
def debug_opensky():
    """تشخيص كامل لاتصال OpenSky من Railway"""
    results = {}
    
    # Test 1: curl basic
    r1 = subprocess.run(
        ["curl", "-s", "--connect-timeout", "10", "--max-time", "15",
         "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45"],
        capture_output=True, text=True, timeout=20
    )
    results["curl_basic"] = {
        "returncode": r1.returncode,
        "stdout_length": len(r1.stdout),
        "has_data": len(r1.stdout) > 100
    }
    
    # Test 2: curl with python-httpx User-Agent
    r2 = subprocess.run(
        ["curl", "-s", "-A", "python-httpx/0.24.1", "--connect-timeout", "10",
         "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45"],
        capture_output=True, text=True, timeout=20
    )
    results["curl_httpx_ua"] = {
        "returncode": r2.returncode,
        "has_data": len(r2.stdout) > 100
    }
    
    # Test 3: curl HTTP/1.1 only
    r3 = subprocess.run(
        ["curl", "-s", "--http1.1", "--connect-timeout", "10",
         "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45"],
        capture_output=True, text=True, timeout=20
    )
    results["curl_http11"] = {
        "returncode": r3.returncode,
        "has_data": len(r3.stdout) > 100
    }
    
    # Test 4: curl TLS 1.2 only
    r4 = subprocess.run(
        ["curl", "-s", "--tlsv1.2", "--connect-timeout", "10",
         "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45"],
        capture_output=True, text=True, timeout=20
    )
    results["curl_tls12"] = {
        "returncode": r4.returncode,
        "has_data": len(r4.stdout) > 100
    }
    
    # Test 5: python requests
    try:
        import requests
        resp = requests.get(
            "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45",
            timeout=10
        )
        results["python_requests"] = {
            "status_code": resp.status_code,
            "has_data": len(resp.content) > 100
        }
    except Exception as e:
        results["python_requests"] = {"error": str(type(e).__name__), "detail": str(e)}
    
    # Test 6: python httpx
    try:
        import httpx
        resp = httpx.get(
            "https://opensky-network.org/api/states/all?lamin=24&lomin=44&lamax=25&lomax=45",
            timeout=10
        )
        results["python_httpx"] = {
            "status_code": resp.status_code,
            "has_data": len(resp.content) > 100
        }
    except Exception as e:
        results["python_httpx"] = {"error": str(type(e).__name__), "detail": str(e)}
    
    # Test 7: nc port 443
    r7 = subprocess.run(
        ["nc", "-zv", "-w", "5", "opensky-network.org", "443"],
        capture_output=True, text=True, timeout=10
    )
    results["nc_port_443"] = {
        "returncode": r7.returncode,
        "stderr": r7.stderr[:200] if r7.stderr else ""
    }
    
    # Test 8: ping
    r8 = subprocess.run(
        ["ping", "-c", "2", "opensky-network.org"],
        capture_output=True, text=True, timeout=10
    )
    results["ping"] = {
        "returncode": r8.returncode,
        "packet_loss": "100%" if "100% packet loss" in r8.stdout else "0%"
    }
    
    return {
        "environment": "Railway",
        "tests": results,
        "conclusion": _analyze(results)
    }

def _analyze(results: dict) -> str:
    curl_ok = results.get("curl_basic", {}).get("has_data", False)
    requests_ok = results.get("python_requests", {}).get("has_data", False)
    httpx_ok = results.get("python_httpx", {}).get("has_data", False)
    nc_ok = "succeeded" in results.get("nc_port_443", {}).get("stderr", "")
    
    if curl_ok and requests_ok and httpx_ok:
        return "ALL_OK: OpenSky reachable from Railway"
    elif curl_ok and not requests_ok and not httpx_ok:
        return "HTTP_LIBRARY_BLOCK: curl works but Python libraries blocked. Use subprocess.curl"
    elif not curl_ok and not nc_ok:
        return "IP_BLOCK: Railway IP blocked by OpenSky. Need proxy or external worker"
    elif not curl_ok and nc_ok:
        return "TLS_ISSUE: TCP works but TLS/HTTP fails. Check TLS version/HTTP version"
    else:
        return "UNCLEAR: Mixed results - manual analysis needed"
