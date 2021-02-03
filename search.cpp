#include <iostream>
#include <mpi.h>
#include <limits>
#include <filesystem>
#include <queue>
#include <fstream>
#include <sstream>

#include <tokenizer/tokenizer.hpp>

using namespace std;

size_t NUM_DOCS_LIMIT = numeric_limits<size_t>::max();
const int TAG = 0;
const int MASTER = 0;
const int OUTPUT_LIMIT = 5;

const int BUFFER_SIZE = 4 * 1024;

string data_dir;

char END[] = "\0";

char message[BUFFER_SIZE];

MPI_Status status;

vector<string> query_tokens;

double get_score(const string &filepath) {
    map<string, int> tf;
    for (const string &token : query_tokens) {
        tf[token] = 0;
    }
    ifstream fin(filepath);
    stringstream buffer;
    buffer << fin.rdbuf();
    int all = 0;
    for (const string &token : Tokenizer::instance().segment_to_string_list(buffer.str())) {
        ++all;
        if (!tf.count(token)) continue;
        tf[token]++;
    }
    double score = 0;
    for (const auto &it : tf) {
        score += it.second;
    }
    score /= all;
    return score;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Missing data_dir path" << endl;
        return -1;
    }
    data_dir = argv[1];

    if (argc > 2) {
        NUM_DOCS_LIMIT = atoi(argv[2]);
    }

    Tokenizer::instance().initialize("/usr/local/share/tokenizer/dicts/");

    MPI_Init(&argc, &argv);

    int rank;
    int num_procs;
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        string query;
        while (getline(cin, query)) {
            cout << "Processing query = " << query << endl;
            for (int i = 1; i < num_procs; ++i) {
                MPI_Send(query.c_str(), query.size() + 1, MPI_CHAR, i, TAG, MPI_COMM_WORLD);    
            }
            int cnt = 0;
            for (auto &p : filesystem::directory_iterator(data_dir)) {
                // cout << p << ' ' << hash<string>{}(p.path().string()) << endl;
                ++cnt;
                if (cnt > NUM_DOCS_LIMIT) break;
                string filepath = p.path().string(); 
                int h = hash<string>{}(filepath) % (num_procs - 1) + 1;
                MPI_Send(filepath.c_str(), filepath.size() + 1, MPI_CHAR, h, TAG, MPI_COMM_WORLD);
            } 
            for (int i = 1; i < num_procs; ++i) {
                MPI_Send(END, sizeof(END), MPI_CHAR, i, TAG, MPI_COMM_WORLD);    
            }

            priority_queue< pair<double, string> > top_docs;
            for (int i = 1; i < num_procs; ++i) {
                while (true) {
                    MPI_Recv(message, BUFFER_SIZE, MPI_CHAR, i, TAG, MPI_COMM_WORLD, &status);
                    if (message[0] == '\0') {
                        break;
                    }
                    double score;
                    MPI_Recv(&score, 1, MPI_DOUBLE, i, TAG, MPI_COMM_WORLD, &status);
                    top_docs.push({score, string(message)});
                    if (top_docs.size() > OUTPUT_LIMIT) {
                        top_docs.pop();
                    }
                }
            }
            vector< pair<double, string> > result;
            while (!top_docs.empty()) {
                double score = -top_docs.top().first;
                string filepath = top_docs.top().second;
                top_docs.pop();
                result.push_back({score, filepath});
            }
            reverse(result.begin(), result.end());
            cout << "Result = \n";
            int id = 0;
            for (const auto &it : result) {
                cout << ++id << " | " << it.second << " | score=" << it.first << endl;
            }
            cout << endl;
        }
        for (int i = 1; i < num_procs; ++i) {
            MPI_Send(END, sizeof(END), MPI_CHAR, i, TAG, MPI_COMM_WORLD);    
        }
    } else {
        while (true) {
            MPI_Recv(message, BUFFER_SIZE, MPI_CHAR, MASTER, TAG, MPI_COMM_WORLD, &status);
            if (message[0] == '\0') {
                break;
            }
            string query(message);
            query_tokens = Tokenizer::instance().segment_to_string_list(query);
            // cout << rank << " query " << query << " | " << query_tokens.size() << endl;

            priority_queue< pair<double, string> > top_docs;
            while (true) {
                MPI_Recv(message, BUFFER_SIZE, MPI_CHAR, MASTER, TAG, MPI_COMM_WORLD, &status);
                if (message[0] == '\0') {
                    break;
                }
                string filepath(message);
                // cout << rank << " path " << filepath << endl;
                top_docs.push({-get_score(filepath), filepath});
                if (top_docs.size() > OUTPUT_LIMIT) {
                    top_docs.pop();
                }
            }
            while (!top_docs.empty()) {
                string filepath = top_docs.top().second;
                double score = top_docs.top().first;
                top_docs.pop();
                MPI_Send(filepath.c_str(), filepath.size() + 1, MPI_CHAR, MASTER, TAG, MPI_COMM_WORLD);
                MPI_Send(&score, 1, MPI_DOUBLE, MASTER, TAG, MPI_COMM_WORLD);
            }
            MPI_Send(END, sizeof(END), MPI_CHAR, MASTER, TAG, MPI_COMM_WORLD);
        }
    }
    MPI_Finalize();
    
    return 0;
}
