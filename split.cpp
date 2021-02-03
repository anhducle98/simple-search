#include <bits/stdc++.h>

using namespace std;

int main() {
    ios::sync_with_stdio(false); cin.tie(nullptr);

    string title;
    while (getline(cin, title)) {
        // title
        ofstream fout("articles/" + title + ".txt");
        fout << title << '\n';
        string line;
        while (getline(cin, line)) {
            if (line.empty()) {
                break;
            }
            fout << line << '\n';
        }
    }
    return 0;
}
