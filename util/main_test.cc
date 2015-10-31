#include <iostream>
#include <sstream>
#include <string>

#include "leveldb/db.h"
#include "rapidjson/document.h"

using namespace std;

//mac
//static string dbPath = "/home/nakshikatha/Desktop/LevelDB_CQ_Test/TestDB";
//ubuntu
static string dbPath = "/home/mohiuddin/Desktop/LevelDB_CQ_Test/TestDB";

std::string generateJSON(std::string k, std::string v) {
    return "{\"City\": \"" + k + "\",\"State\": \"" + v + "\"}";
}

void print_vals(std::vector<leveldb::KeyValuePair>& vals) {
  for(std::vector<leveldb::KeyValuePair>::iterator it = vals.begin(); it != vals.end(); ++it)
    cout << "key: " << it->key.data() << " value: " << it->value.data() << std::endl;
}


void test_init(string dbpath)
{
    
    //
    //************************************************************************************
    leveldb::DB* db;
    leveldb::Options options;

    options.create_if_missing = true;
    options.using_s_index = true;
    options.primary_key = "City";
    options.secondary_key = "State";
    options.isSecondaryDB = false;
    options.using_s_index = true;
    options.isSecondaryDB = false;

    std::cout << "Trying to create database\n";

    leveldb::Status status = leveldb::DB::Open(options, dbpath , &db);
    assert(status.ok());

    std::cout << "Created databases\n";

    leveldb::WriteOptions woptions;
    std::string val;

    std::cout << "Trying to write values\n";

    val = generateJSON("Riverside", "California");
    leveldb::Status s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("San Diego", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Miami", "Florida");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Illinois");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Springfield", "Massachusetts");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Los Angeles", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Boston", "Massachusetts");
    s = db->Put(woptions, val);
    assert(s.ok());

    val = generateJSON("Irvine", "California");
    s = db->Put(woptions, val);
    assert(s.ok());

    //*/
    std::cout << "\nFinished writing values\n";




    //* // read them back
    leveldb::ReadOptions roptions;
    std::string skey;
    std::vector<leveldb::KeyValuePair> ret_vals;

    skey = "California";
    roptions.num_records = 5;
    leveldb::Status s2 = db->Get(roptions, skey, &ret_vals);
    assert(s2.ok());
    print_vals(ret_vals);
    cout<<ret_vals.size()<<endl;

    ret_vals.clear();
    skey = "Florida";
    roptions.num_records = 2;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);
    cout<<ret_vals.size()<<endl;

    ret_vals.clear();
    skey = "Illinois";
    roptions.num_records = 4;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);
    cout<<ret_vals.size()<<endl;

    ret_vals.clear();
    skey = "Massachusetts";
    roptions.num_records = 3;
    db->Get(roptions, skey, &ret_vals);
    print_vals(ret_vals);
    cout<<ret_vals.size()<<endl;
    //*/
    std::cout << "\nFinished reading values\n";
}

/*
 * 
 */
int main(int argc, char** argv) {

    test_init(dbPath);
    return 0;
}

