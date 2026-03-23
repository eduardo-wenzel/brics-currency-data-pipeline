from pipeline.migrations import apply_migrations

if __name__ == "__main__":
    apply_migrations(force=True)
