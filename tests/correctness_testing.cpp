#include <Rcpp.h>
using namespace Rcpp;
// [[Rcpp::plugins(cpp11)]]

// [[Rcpp::export(rng=false)]]
CharacterVector splitstr(std::string x, std::vector<double> cuts){
  CharacterVector ret(cuts.size() - 1);
  for(uint64_t i=1; i<cuts.size(); i++) {
    ret[i-1] = x.substr(std::round(cuts[i-1])-1, std::round(cuts[i])-std::round(cuts[i-1]));
  }
  return ret;
}

// [[Rcpp::export(rng=false)]]
int setlev(SEXP x, int i) {
  return SETLEVELS(x,i);
}

// [[Rcpp::export(rng=false)]]
void setobj(SEXP x, int i) {
  return SET_OBJECT(x, i);
}

// [[Rcpp::export(rng=false)]]
List generateList(std::vector<int> list_elements){
  auto randchar = []() -> char
  {
    const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[ rand() % max_index ];
  };
  List ret(list_elements.size());
  std::string str(10,0);
  for(size_t i=0; i<list_elements.size(); i++) {
    switch(list_elements[i]) {
    case 1:
      ret[i] = R_NilValue;
      break;
    case 2:
      std::generate_n( str.begin(), 10, randchar );
      ret[i] = str;
      break;
    case 3:
      ret[i] = rand();
      break;
    case 4:
      ret[i] = static_cast<double>(rand());
      break;
    }
  }
  return ret;
}