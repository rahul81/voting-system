from user_generator import Generator
from utils import file_writer

# defining number of voters to generate
NUMBER_OF_VOTERS = 1000
GENERATION_SEED = 'voters'
NATIONALITY = 'US'

# randomuser.me generates user, below url will generate same user everytime as per given seed value and Nationality
VOTERS_GEN_API_URL = f"https://randomuser.me/api/?seed={GENERATION_SEED}&results={NUMBER_OF_VOTERS}&nat={NATIONALITY}"

DATA_PATH = './data'
OUTPUT_CSV_PATH = f"{DATA_PATH}/voters.csv"

csv_header = [
    "title",
    "firstName",
    "lastName",
    "gender",
    "age",
    "city",
    "state",
    "country",
    "postal_code"
    ]


writer = file_writer(OUTPUT_CSV_PATH, 'w')

writer.writerow(csv_header)

voters_generator = Generator()

voters = voters_generator.generate_user(
    api_url=VOTERS_GEN_API_URL
)

for candidate in voters:
    writer.writerow(list(candidate.values()))


print(f"Successfully written data to {OUTPUT_CSV_PATH}")


