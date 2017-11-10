C_CHANGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep -Ee "\.[ch]$")
CXX_CHANGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep -Ee "\.([chi](pp|xx)|(cc|hh|ii)|[CHI])$")
CLANG_FORMAT=$(command -v clang-format)
$CLANG_FORMAT -i -style=file ${CXX_CHANGED_FILES}
