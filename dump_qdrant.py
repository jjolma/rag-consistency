"""Print every point in the Qdrant collection (id + payload)."""
from qdrant_client import QdrantClient

client = QdrantClient(url="http://localhost:6333")
points, _ = client.scroll("articles", limit=1000, with_payload=True, with_vectors=False)
print(f"{len(points)} point(s)")
for p in sorted(points, key=lambda p: p.id):
    print(p.id, p.payload)
