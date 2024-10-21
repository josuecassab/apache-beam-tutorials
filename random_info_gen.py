from faker import Faker
fake = Faker()

ls = []

for i in range(10):
    ls.append(fake.simple_profile())