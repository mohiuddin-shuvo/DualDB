#include <iostream>
#include <sstream>
#include <string>
#include <fstream>
#include <vector>
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/prettywriter.h"	// for stringify JSON
#include "rapidjson/filestream.h"
#include <leveldb/slice.h>
#include <sys/time.h>

using namespace std;
using namespace rapidjson;

#define DEPTH 5

enum TYPEOFEXP
{
	SIMPLE,
	SELFJOIN,
	DEWEY
};


//server
//static string dbPath = "/home/mabdu002/ContinuousQuery/DB/BenchmarkDualDB";
//static string benchmarkPath = "/home/mabdu002/ContinuousQuery/Benchmark/RD_CQ1_Q50";
//static string result_filePath = "/home/mabdu002/ContinuousQuery/Results/DualDB_RD_Q50_result.csv";

//ubuntu
static string dbPath = "/home/mohiuddin/Desktop/LevelDB_CQ_Test/DB/DualDB_NoDewey_Radius_NR_Q50";
static string benchmarkPath = "/home/mohiuddin/Desktop/LevelDB_CQ_Test/Benchmark/NoDeweyRadiusDataset_NR_Q50";
static string result_filePath = "/home/mohiuddin/Desktop/LevelDB_CQ_Test/Results/DualDB_NoDewey_Radius_NR_Q50";
static int numberofiterations = 2;
static long LOG_POINT = 500000;

bool SetAttr(rapidjson::Document& doc, const char* attr, long value) {
  if(!doc.IsObject() || !doc.HasMember(attr) || doc[attr].IsNull())
    return false;

  std::ostringstream pKey;
  //cout<<value << " ";
  string val =  std::to_string(value);
  //cout<<val<<endl;

	Value v;
	v.SetString(val.c_str(), val.length(), doc.GetAllocator());
	doc.RemoveMember(attr);
	doc.AddMember(attr,  v , doc.GetAllocator());
   //doc[attr].SetString(val.c_str());
   return true;

}
std::string generateJSON(std::string k, std::string v) {
    return "{\"City\": \"" + k + "\",\"State\": \"" + v + "\"}";
}

void print_vals(std::vector<string>& vals) {
  for(std::vector<string>::iterator it = vals.begin(); it != vals.end(); ++it)
    cout << "Users: " << *it << " ";
  cout<<std::endl;
}

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

std::string parse_json (std::string json)
{
	rapidjson::Document document;
	document.Parse<0>(json.c_str());

	long int sec= time(NULL);

	//cout<<sec<<endl;

	bool tmin = SetAttr(document, "Tnow", sec);

	tmin = SetAttr(document, "Tmin", sec-60);

	if(tmin) //Query
		tmin = SetAttr(document, "Tmax", sec+60);
	else //Event
		tmin = SetAttr(document, "Tmax", sec+95);

	rapidjson::GenericStringBuffer<rapidjson::UTF8<>> buffer;
	rapidjson::Writer< rapidjson::GenericStringBuffer< rapidjson::UTF8<> > > writer(buffer);
	document.Accept(writer);
	std::string st=buffer.GetString();
	return st;
}



void test_frombenchmark(int typeofexp, int loop)
{
    
    //
    //************************************************************************************
    leveldb::DB* db;
    leveldb::Options options;

    long sizeperblock = 8*1024;
    size_t cachesize = 2L * 100L * 1000L * 1000L * 1024L; //200MB

    options.block_size = sizeperblock;

    options.block_cache = leveldb::NewLRUCache(cachesize);  //200 MB

    options.create_if_missing = true;
//    options.using_s_index = true;
//    options.primary_key = "City";
//    options.secondary_key = "State";
//    options.isQueryDB = false;

    ifstream ifile(benchmarkPath.c_str());
    if (!ifile) { cerr << "Can't open input file " << endl; return; }
    ofstream ofile((result_filePath+".csv").c_str(),  std::ofstream::out | std::ofstream::app );
    if (!ofile) { cerr << "Can't open output file " << endl; return; }
    ofstream* ofiles;


    if(typeofexp==TYPEOFEXP::DEWEY)
	{
    	 ofiles = new ofstream[DEPTH];
		for(int j=0 ;j <DEPTH; j++)
		{
			//cout<< Deweydata[i];
			//cout<<Deweyquery;
//			/std::string path = result_filePath+ "" + (j+1);
			ofiles[j].open((result_filePath + "_" + std::to_string(j+1) + ".csv").c_str() , std::ofstream::out | std::ofstream::app );
		}

	}

    std::cout << "Trying to create database\n";

    leveldb::Status status = leveldb::DB::Open(options, dbPath , &db);
    assert(status.ok());

    string line;
	//int i=0;
	rapidjson::Document d;
	leveldb::ReadOptions roptions;
	double data=0, query=0;
	double durationData=0,durationQuery=0;


	vector<string> events;
	vector<string> users;
	double * Deweydata , *Deweyquery ;
	double * Deweyresultsdata , *Deweyresultsquery ;
	double * DeweydurationData, *DeweydurationQuery;
	if(typeofexp==TYPEOFEXP::DEWEY)
	{
		Deweydata = new double [DEPTH];
		Deweyquery = new double [DEPTH];
		DeweydurationData = new double [DEPTH];
		DeweydurationQuery = new double [DEPTH];
		Deweyresultsdata = new double [DEPTH];
		Deweyresultsquery = new double [DEPTH];

		for(int i=0 ;i <DEPTH; i++)
		{
			Deweydata[i] = 0;
			Deweyquery[i]=0;
			DeweydurationData[i]=0;
			DeweydurationQuery[i]=0;
			Deweyresultsdata[i] = 0;
			Deweyresultsquery[i] = 0;
		}

	}

	long userc = 0, eventc = 0;
	long index = 0;
	int depth = 0;
	long i =0;

	long linecount = 0;
	for(int itr = 0 ; itr<loop; itr++)
	{
		while(getline(ifile, line)) {

			//i++;
			//linecount++;

			std::vector<std::string> x = split(line, '\t');
			leveldb::WriteOptions woptions;
			struct timeval start, end;
			if(x.size()==5) {
				index = std::stol(x[1]);
				depth = std::stoi(x[2]);
				string keys = x[3];
				leveldb::Slice key = keys;
				if(x[1].empty())
					continue;
				//cout<<parse_json(x[2])<<endl;
				//continue;
				string json_string = parse_json(x[4]);
				leveldb::Slice json_value = json_string;
				//cout<< key.ToString()<<" -> ";
				//cout<<json_value.ToString()<<endl;

	//			if(index >= 1000)
	//				break;
	//			cout<<x[0]<<endl;
	//			cout<<x[1]<<endl;
	//			cout<<x[3]<<endl;
	//			cout<<x[4]<<endl;
				gettimeofday(&start, NULL);
				if(x[0]=="D") {

					leveldb::Status s;
					if(typeofexp==TYPEOFEXP::SELFJOIN)
						s = db->PublishAndSelfJoin(woptions,key, json_value, users, events );
					else if(typeofexp==TYPEOFEXP::SIMPLE)
						s = db->Publish(woptions, key , json_value, users);
					else if(typeofexp==TYPEOFEXP::DEWEY)
					{
						s = db->PublishInDewey(woptions, key , json_value, users);

					}
					gettimeofday(&end, NULL);
					if(typeofexp==TYPEOFEXP::DEWEY)
					{
						//int s = split(x[1], '.').size()-1;

						DeweydurationData[depth-1] += ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));

						if(i!=index)
						{
							data++;
							Deweydata[depth-1]++;
						}
						Deweyresultsdata[depth-1] += users.size();
					}

					durationData+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					//print_vals(users);
					//cout<<"Users: "<<users.size()<<endl;

					if(users.size()>0)
					{
						//cout<<"Users: "<<users.size()<<endl;
						userc+=users.size();
						users.clear();


					}
					if(events.size()>0)
					{
						//cout<<"Events: "<<events.size()<<endl;
						events.clear();
					}

				}
				else if(x[0]=="Q")
				{
					leveldb::Status s;
					if(typeofexp==TYPEOFEXP::SELFJOIN)
						s = db->SubscribeAndSelfJoin(roptions,key, json_value, users, events );
					else if(typeofexp==TYPEOFEXP::SIMPLE)
						s = db->Subscribe(roptions, key , json_value, events);
					else if(typeofexp==TYPEOFEXP::DEWEY)
					{
						s = db->SubscribeInDewey(roptions, key , json_value, events);

					}
					gettimeofday(&end, NULL);
					if(typeofexp==TYPEOFEXP::DEWEY)
					{
						//int s = split(x[1], '.').size()-1;
						DeweydurationQuery[depth-1] += ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
						if(i!=index)
						{
							query++;
							Deweyquery[depth-1]++;
						}
						Deweyresultsquery[depth-1] += events.size();
					}

					//leveldb::Status s = db->Subscribe(roptions, key , json_value, events);
					//leveldb::Status s = db->SubscribeAndSelfJoin(roptions, key , json_value, users, events );

					//gettimeofday(&end, NULL);
					durationQuery+= ((end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
					//print_vals(events);

					//cout<<"Events: "<<events.size()<<endl;
					if(users.size()>0)
					{
	//					//cout<<"Users: "<<users.size()<<endl;
						users.clear();
	//
					}
					if(events.size()>0) {
						//cout<<"Events: "<<events.size()<<endl;
						eventc+=events.size();
						events.clear();
					}

				}
			}

			if(i!=index)
			{
				i = index;
				linecount++;
		//		if(i==20)
		//			break;
				if (linecount%LOG_POINT == 0) {
					//cout<<corr<<endl;
		//			ofile<<"i: "<<i<<endl;
		//			ofile<<"Number of Queries: "<<query<<endl;
		//			ofile<<"Total Query duration: "<<durationQuery<<endl;
		//			ofile<<"Time per Query: "<<durationQuery/query<<endl<<endl;
		//
					//cout<<i<<endl<<"Time per Query: "<<durationQuery/query<<endl;
		//
		//			ofile<<"Number of Events: "<<data<<endl;
		//			ofile<<"Total Event duration: "<<durationData<<endl;
		//			ofile<<"Time per Event: "<<durationData/data<<endl<<endl;
		//
					//cout<<"Time per Data: "<<durationData/data<<endl<<endl;
		//
		//			ofile<<"Number of operation: "<<data+query<<endl;
		//			ofile<<"Time taken of all operation: "<<(durationData+durationQuery)<<endl;
		//			ofile<<"Time per operation: "<<(durationData+durationQuery)/(data+query)<<endl<<endl<<endl;

					//cout<< userc/data <<","<<eventc/query<<endl;
					if(linecount%LOG_POINT==0)
					{
						if(linecount/LOG_POINT==1)
						{
							ofile << "No of Op (Millions)" <<"," << "Time Per Op (MicroSec)" <<"," << "No of Queries (Millions)" <<"," << "Time Per SUBSCRIBE op (MicroSec)" <<"," << "No of Events (Millions)" <<"," << "Time Per PUBLISH op (MicroSec)" << ","<< "Results per SUB Query" <<","<<  "Results per PUB Query" <<endl;
						}

						ofile<< linecount/LOG_POINT <<"," << (durationData+durationQuery)/linecount <<"," << query/LOG_POINT <<"," << durationQuery/query <<"," << data/LOG_POINT <<"," << durationData/data << ","<< eventc/query  <<","<< userc/data <<endl;


						if(typeofexp==TYPEOFEXP::DEWEY)
						{

							for(int j=0 ;j <DEPTH; j++)
							{
								//cout<< Deweydata[i];
								//cout<<Deweyquery;
								if(linecount/LOG_POINT==1)
								{
									ofiles[j] << "No of Op (Millions)" << ","<< "Time Per Op" <<"," << "Time Per SUBSCRIBE op (MicroSec)" <<"," << "Time Per PUBLISH op (MicroSec)" << ","<< "Results per SUB Query" <<","<<  "Results per PUB Query" <<endl;
								}

								// (i/1000000) * ((Deweydata[j]+Deweyquery[j])/i) *100.0
								ofiles[j]<< linecount/LOG_POINT <<"," << (DeweydurationQuery[j]+DeweydurationData[j])/(Deweyquery[j]+Deweydata[j]) <<"," << DeweydurationData[j]/Deweydata[j] <<","  << DeweydurationQuery[j]/Deweyquery[j] <<"," << Deweyresultsquery[j]/Deweyquery[j] <<","<<Deweyresultsdata[j]/Deweydata[j]<<endl;

							}

						}



					}
					cout<< linecount <<"," << (durationData+durationQuery)/linecount <<"," << query <<"," << durationQuery/query <<"," << data <<"," << durationData/data << ","<< userc/data <<","<<eventc/query<<endl;

					cout<< userc/data <<","<<eventc/query<<endl;

					if(typeofexp==TYPEOFEXP::DEWEY)
					{

						for(int j=0 ;j <DEPTH; j++)
						{
							//cout<< Deweydata[i];
							//cout<<Deweyquery;
							cout<<"Depth: "<<j+1<<endl;
							cout<<"Time Per Op: "<<endl;
							cout<<"Publish: ";
							cout<<DeweydurationData[j]/Deweydata[j];
							cout<<", Subscribe: ";
							cout<<DeweydurationQuery[j]/Deweyquery[j];
							cout<<endl<<endl;

							cout<<"Result Per Op: "<<endl;
							cout<<"Publish: ";
							cout<<Deweyresultsdata[j]/Deweydata[j];
							cout<<", Subscribe: ";
							cout<<Deweyresultsquery[j]/Deweyquery[j];

							cout<<endl<<endl;
						}

					}


				}
			}
		}

		ifile.clear();
		ifile.seekg(0, ios::beg);
	}

    delete db;
    delete options.block_cache;

}

void testingRapidJson ()
{

	leveldb::Slice val =  "{ \"hello\" : \"world\" }";

	std::string new_key_list = "[";
	new_key_list += ("\"" + val.ToString() + "\"");
	new_key_list += "]";

	rapidjson::Document document;
	document.Parse<0>(new_key_list.c_str());

//
//	PrettyWriter<FileStream> writer(f);
//	document.Accept(writer);
	GenericStringBuffer<UTF8<>> buffer;
//	rapidjson::GenericStringBuffer<rapidjson::UTF8<>> buffer;
//
	Writer< GenericStringBuffer< UTF8<> > > writer(buffer);
	document.Accept(writer);
	std::string st=buffer.GetString();


	cout<<document.Size();
	int i = document.Size() - 1;
		  //  while (kNoOfOutputs>0&& i >= 0) {
	while ( i >= 0)
	{


		if(document[i].IsObject())
		{
			GenericStringBuffer<UTF8<>> buffer;
			Writer< GenericStringBuffer< UTF8<> > > writer(buffer);
			document[i].Accept(writer);
			std::string st=buffer.GetString();
			cout<<st;
		}

		i--;
	}
}

/*
 *
 */
int main(int argc, char** argv) {


	if(argc == 4)
	{
		benchmarkPath = argv[1];
		dbPath = argv[2];
		result_filePath = argv[3];
		//cout<< argv[3] ;
		test_frombenchmark(TYPEOFEXP::DEWEY, numberofiterations);

	}
	else
	{
		test_frombenchmark(TYPEOFEXP::DEWEY, numberofiterations);
		cout<<"Please Put arguments in order: \n arg1=benchmarkpath arg2=dbpath arg3=resultpath\n";
	}
	//cout<<"\nasdasd\n";
	//else



	//	string cellstring  = "2.3.5Q";
//	string endkey = "2.3.5";
//	cellstring.substr(0, cellstring.size()-1).compare(endkey) >= 0 ? cout <<"break": cout<<"continue";
	//cout<<"Compile_DualDB!\n";
	return 0;
}

