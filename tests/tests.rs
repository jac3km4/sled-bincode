use bincode::{BorrowDecode, Encode};
use sled_bincode::{ConflictableTransactionError, Transactional, Tree, TreeEntry};
use temp_dir::TempDir;

#[derive(Debug, PartialEq, BorrowDecode, Encode)]
struct Person<'a> {
    name: &'a str,
    age: u32,
}

struct PersonEntry;

impl<'a> TreeEntry<'a> for PersonEntry {
    type Key = &'a str;
    type Val = Person<'a>;
}

fn test_tree() -> Tree<PersonEntry> {
    let dir = TempDir::new().unwrap();
    let db = sled::open(dir.path()).unwrap();
    Tree::open(&db, "people").unwrap()
}

#[test]
fn insert_and_get_works() {
    let tree = test_tree();
    let person = Person {
        name: "John",
        age: 32,
    };
    tree.insert(&person.name, &person).unwrap();
    let retrieved = tree.get(&person.name).unwrap();
    assert_eq!(person, retrieved.unwrap().value().unwrap());
}

#[test]
fn iter_works() {
    let tree = test_tree();
    let person = Person {
        name: "John",
        age: 32,
    };
    tree.insert(&"Paul", &person).unwrap();
    tree.insert(&"Adam", &person).unwrap();
    tree.insert(&"Jane", &person).unwrap();

    let mut iter = tree.iter();

    let kv = iter.next().unwrap().unwrap();
    assert_eq!(kv.key().unwrap(), "Adam");
    assert_eq!(kv.value().unwrap(), person);

    let kv = iter.next().unwrap().unwrap();
    assert_eq!(kv.key().unwrap(), "Jane");
    assert_eq!(kv.value().unwrap(), person);

    let kv = iter.next().unwrap().unwrap();
    assert_eq!(kv.key().unwrap(), "Paul");
    assert_eq!(kv.value().unwrap(), person);
}

#[test]
fn transaction_works() {
    let dir = TempDir::new().unwrap();
    let db = sled::open(dir.path()).unwrap();
    let tree1: Tree<PersonEntry> = Tree::open(&db, "tree1").unwrap();
    let tree2: Tree<PersonEntry> = Tree::open(&db, "tree2").unwrap();
    let tree3: Tree<PersonEntry> = Tree::open(&db, "tree3").unwrap();

    let person = Person {
        name: "John",
        age: 32,
    };

    (&tree1, &tree2, &tree3)
        .transaction(|t1, t2, t3| {
            t1.insert(&person.name, &person)?;
            t2.insert(&person.name, &person)?;
            t3.insert(&person.name, &person)?;
            Ok::<_, ConflictableTransactionError>(())
        })
        .unwrap();

    assert_eq!(tree1.get(&person.name).unwrap().unwrap().value().unwrap(), person);
    assert_eq!(tree2.get(&person.name).unwrap().unwrap().value().unwrap(), person);
    assert_eq!(tree3.get(&person.name).unwrap().unwrap().value().unwrap(), person);
}
