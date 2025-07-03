#include <bits/stdc++.h>
#include <pthread.h>
#include <mutex>
using namespace std;
using namespace chrono;//for using the clock timestamp
typedef long long int ll;


map<ll,ll> visited;
queue<ll> q;
mutex lock1;
map<ll,vector<ll>> adj;
map<ll,vector<ll>> order_of_traversal;

std::ofstream log_file("output.txt");//file to writing the final logs 

//struct to handle the arguments to the thread
struct argument
{
    ll thread_id;
    vector<pair<ll,string>> v;
    ll m;
    ll total_tasks;
};

map<ll,ll> marked;

//this function is the threaded implementation of bfs
void do_work(struct argument* &temp)
{
    
    if(q.empty()) return;
    ll node=q.front();
    q.pop();
    
    for(auto child:adj[node])
    {
        if(visited[child]==0)
        {
            q.push(child);

                order_of_traversal[node].push_back(child);

            visited[child]=1;
        }
    }
    temp->v.push_back({node,"Vertex "+to_string(node)+" discovered by thread "+to_string(temp->thread_id)});
    log_file<<"Vertex "+to_string(node)+" discovered by thread "+to_string(temp->thread_id)<<endl;
}

//this function is used to assign work to each thread
void *worker(void* arg)
{
    argument *temp = (argument *)arg;
    ll thread_id = temp->thread_id;
    std::lock_guard<std::mutex> cs_lock(lock1);
    for(ll i=0;i<temp->total_tasks;i++)
    {
        do_work(temp);
    }
    pthread_exit(0);
}


//function to parse the lines form the file 
vector<ll> parse(string line){
    int l=0;
    vector<ll> v;
    for(int i=0;i<line.size();i++){
        if(line[i]==' '){
            v.push_back(stoll(line.substr(l,i-l)));
            l=i+1;
        }
    }
    v.push_back(stoll(line.substr(l)));
    return v;
}


//main driver code
int main(int argc,char* argv[])
{

    ifstream file1(argv[1]);
    if (!file1) //if there is error in opening file then report 
    {
        cerr << "Error opening one or both files." << endl;
        return 1;
    }
    ll m,n;

    string line;
    getline(file1,line);
    vector<ll> vi = parse(line);
    m=vi[0];n=vi[1];
    while (getline(file1, line))
    {
        vector<ll> vi = parse(line);
        if(vi.size()>=2) //condition check wheather atleast 1 neighbour of a node is pressent or not
        {
            for(ll i=1;i<vi.size();i++)
            {
                adj[vi[0]].push_back(vi[i]);
            }
        }
        
    }
    // q.push(1);
    // visited[1]=1;
    
    //this is for the initialization step of the bfs
    q.push(adj.begin()->first);
    visited[adj.begin()->first]=1;

    argument args[m];
    pthread_t tids[m];
    // calculating total tasks each thread has to do
    ll tasks_each;
    if(m>n)
    {
        tasks_each=1;
    }
    else if(m<n)
    {
        // tasks_each=n/m+1;

            tasks_each=n/m;

    }
    else
    {
        tasks_each=1;
    }
    // record overall simulation start time.
    auto start = chrono::high_resolution_clock::now();

    for (ll i = 0; i < m; i++)
    {
        pthread_attr_t attr;
        args[i].thread_id = i+1;
        args[i].m = m;
        args[i].total_tasks=tasks_each;
        if(i==m-1 && n%m!=0)
        {
            tasks_each+=n%m;
            args[i].total_tasks=tasks_each;
        }
        // args[i].tasc_inc = taskInc;
        pthread_attr_init(&attr);
        pthread_create(&tids[i], &attr, worker, &args[i]);
    }


    for (int i = 0; i < m; i++)
    {
        pthread_join(tids[i], NULL);
    }

    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    // cout<<order_of_traversal.size()<<endl;
    for(auto [x,y]:order_of_traversal)
    {
        log_file<<x<<" ";
        for(auto j:y)
        {
            log_file<<j<<" ";
        }
        log_file<<endl;
    }
    log_file << "The total simulation time is " << duration << " microseconds\n";

    //debugging steps here
    // for(auto i:args)
    // {
    //     for(auto j:i.v)
    //     {
    //         if(j.second.size()==0) continue;
    //         cout<<j.second<<" "<<  j.second.size()<<endl;
    //     }
    // }
    
    return 0;
}