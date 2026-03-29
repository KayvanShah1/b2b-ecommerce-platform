from b2b_ec_utils.storage import storage


def nuke(bucket_name: str = None):
    print(f"Deleting {bucket_name}...")
    storage.fs.rm(bucket_name, recursive=True)


if __name__ == "__main__":
    nuke("b2b-ec-marketing-leads")
