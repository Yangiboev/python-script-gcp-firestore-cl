import collections
from google.cloud import firestore
import sys
import string

def delete_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f'\nDeleting doc {doc.id} => {doc.to_dict()}\n')
        doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)
    return deleted

print('Welcome! The program drop the collections of specified cluster below!')
projectID = input("Please, enter your project ID: ")
prefix = input("Please, enter your cluster name: ")

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.
db = firestore.Client(project=projectID)
docs = db.collections()
collectionList = list()
print(f'\n{docs}!\n')
for doc in docs:
    if(doc.id.startswith(prefix)):
        collectionList.append(doc)
if len(collectionList) == 0:
    print(f'\nNo collections found in {prefix} cluster, Thank you!\n')
else:
    for coll in collectionList:
        print(f'\n{coll.id}\n')
    confirm = input("Above, Collections will be deleted \nPlease, enter your confirmation(y,n) (default: y): ") or "y"
    if confirm == 'y':
        deleted = 0
        for coll in collectionList:
            # Since a batched write can contain up to 500 operations, the maximum value you can assign to batchSize is 500.
            deleted += delete_collection(coll, 500)
            print(f'\n{coll.id} was deleted!\n')
        print(f'\n{deleted} collections found and deleted in {prefix} cluster!\n')

    else:
        print(f'\nNo collections deleted, Thank you!\n')

