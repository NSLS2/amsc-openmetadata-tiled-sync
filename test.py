from amsc_openmetadata_tiled_sync.main import listen

print("Starting tiled Listener")
listen("https://tiled.nsls2.bnl.gov/api/v1/metadata/tst/sandbox/qas/processed")
