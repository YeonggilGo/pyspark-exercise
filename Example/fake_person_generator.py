from faker import Faker

fake = Faker()

N = 5000

with open("../src/output/fake_person.txt", "w") as file:
    for i in range(N):
        name = fake.name()
        age = fake.random_int(18, 55)
        friends = fake.random_int(0, 500)  # number of friends this person has
        file.write(f"{i}, {name}, {age}, {friends}\n")
