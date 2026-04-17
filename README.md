# AmSC OpenMetadata - Tiled Sync

1. Find **ID token** at the bottom of [https://my.american-science-cloud.org/profile](https://my.american-science-cloud.org/profile) (after logging in with Globus).

2. Make a `.env` file (see `.env.example`) with that ID token and a catalog name.

3. Start a Tiled subscription that sends a POST to OpenMetadata when a new node is created in Tiled's sandbox.

   ```python
   uv run test.py
   ```

4. Then, write to the sandbox like:

   ```python
   c['tst/sandbox'].write_array([1,2,3], metadata={'color': 'blue', 'mood': 'elated', 'description': 'Time for some American Science, amiright?', 'uid': str(uuid.uuid4())})
   ```
