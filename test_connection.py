from shared.database import test_connection

if __name__ == "__main__":
    print("Testing database connection...")
    if test_connection():
        print("✅ Database connection successful!")
    else:
        print("❌ Database connection failed!")