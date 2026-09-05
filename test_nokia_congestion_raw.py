import network_as_code as nac
from network_as_code.models.device import Device
from config import get_settings


print("=" * 60)
print("HARIS - NOKIA RAW CONGESTION TEST")
print("=" * 60)

settings = get_settings()

if not settings.nac_api_token:
    raise RuntimeError("NAC_API_TOKEN is not available.")

token = settings.nac_api_token.get_secret_value()

print("NAC token: AVAILABLE")

client = nac.NetworkAsCodeClient(token=token)

device = Device(
    api=client._api,
    phone_number="+999900000001",
)

print()
print("Device:")
print(device)

print()
print("Requesting RAW Nokia congestion response...")

result = client._api.congestion.fetch_congestion(
    device=device,
)

print()
print("RAW NOKIA CONGESTION RESPONSE:")
print(result)

print()
print("=" * 60)
print("TEST COMPLETED")
print("=" * 60)