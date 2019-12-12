# Build instructions

You need CMake v3.10 or latter.

_Note: For CentOS 7, use the `cmake3` package available in EPEL. Replace the following commands with `cmake3`._

- **Step 1:** Crate and enter build directory
    - **`mkdir -p build && cd build`**
- **Step 1:** Configure CMAKE and generate makefiles (only once)
    - **`cmake .`**
- **Step 3:** Compile the project
    - **`make`**
- **Step 4:** Install libraries [Optional]
    - **`sudo make install`**
- **Step 5:** Run tests [Optional] 
    - **`make test`**
