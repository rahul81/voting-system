import requests

#helper function

class Generator():

    def __init__(self) -> None:
        return

    def generate_user(
            self,
            api_url:str) :

        print(f"Fetching data from url >> {api_url}")

        response = requests.get(api_url)

        if response.status_code == 200:

            print("Users data fetched !!")

            users = response.json()['results']

            for user in users:

                user_data = {
                    "title": user['name']['title'],
                    "firstName": user['name']['first'],
                    "lastName": user['name']['last'],
                    "gender": user['gender'],
                    "age": user['dob']['age'],
                    "city": user['location']['city'],
                    "state": user['location']['state'],
                    "country": user['location']['country'],
                    "postal_code": user['location']['postcode'],
                }

                yield user_data

