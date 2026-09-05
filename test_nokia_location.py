import os

from dotenv import load_dotenv
import network_as_code as nac


load_dotenv()

token = os.getenv("NAC_API_TOKEN")

if not token:
    raise RuntimeError("NAC_API_TOKEN is missing from .env")


print("=" * 60)
print("HARIS - NOKIA SIMULATOR LOCATION TEST")
print("=" * 60)
print("NAC token: AVAILABLE")
print()


# ---------------------------------------------------------
# Create Nokia Network as Code client
# ---------------------------------------------------------
client = nac.NetworkAsCodeClient(token=token)

print("Nokia Network as Code client: CREATED")


# ---------------------------------------------------------
# HARIS test device
# ---------------------------------------------------------
device = client.devices.get(
    phone_number="+999900000001"
)

print("Simulator device:")
print(device)
print()


# ---------------------------------------------------------
# Location Retrieval API
#
# The installed SDK exposes the API internally as:
#
# client._api.location_retrieve
#
# The public NetworkAsCodeClient does not expose
# a client.location namespace in this SDK version.
# ---------------------------------------------------------
location_api = client._api.location_retrieve

print("Location Retrieval API:")
print(type(location_api))
print()


# ---------------------------------------------------------
# Read-only location request
# ---------------------------------------------------------
print("Requesting device location...")

result = location_api.get_location(
    device=device,
    max_age=60,
)

print()
print("NOKIA LOCATION RESPONSE:")
print(result)

print()
print("=" * 60)
print("TEST COMPLETED")
print("=" * 60)