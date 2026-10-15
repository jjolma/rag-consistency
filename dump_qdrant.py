"""Print every point in the Qdrant collection (id + payload)."""
from qdrant_client import QdrantClient

from common import COLLECTION, QDRANT_URL

client = QdrantClient(url=QDRANT_URL)
points, _ = client.scroll(COLLECTION, limit=1000, with_payload=True, with_vectors=False)
print(f"{len(points)} point(s)")
for p in sorted(points, key=lambda p: (p.payload["article_id"], p.payload["chunk_index"])):
    print(p.id, p.payload)
