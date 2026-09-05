import os

from dotenv import load_dotenv
import network_as_code as nac


load_dotenv()

token = os.getenv("NAC_API_TOKEN")

if not token:
    raise RuntimeError("NAC_API_TOKEN is missing from .env")


print("=" * 60)
print("HARIS - NOKIA SIMULATOR DEVICE STATUS TEST")
print("=" * 60)
print("NAC token: AVAILABLE")
print()


# ---------------------------------------------------------
# Create Nokia client
# ---------------------------------------------------------
client = nac.NetworkAsCodeClient(token=token)

print("Nokia Network as Code client: CREATED")


# ---------------------------------------------------------
# HARIS T03 -> Nokia Simulator Device
# ---------------------------------------------------------
device = client.devices.get(
    phone_number="+999900000001"
)

print("Simulator device:")
print(device)
print()


# ---------------------------------------------------------
# Use the Reachability API already configured
# inside the Nokia Network as Code client.
# ---------------------------------------------------------
reachability_api = client.device_status.api.reachability_status

print("Reachability API:")
print(type(reachability_api))
print()


# ---------------------------------------------------------
# Read-only request
# ---------------------------------------------------------
print("Requesting device reachability status...")

result = reachability_api.get_reachability(
    device.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
)

print()
print("NOKIA DEVICE STATUS RESPONSE:")
print(result)

print()
print("=" * 60)
print("TEST COMPLETED")
print("=" * 60)