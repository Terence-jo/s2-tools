FROM python:3.12-bullseye

RUN apt-get update \
    && apt-get install -y python3 cmake swig googletest libssl-dev && mkdir /build && mkdir /install

WORKDIR /usr/local/src

ENV GTEST_VERSION=1.14.0
RUN curl -Lo googletest-v${GTEST_VERSION}.tar.gz https://github.com/google/googletest/archive/refs/tags/v${GTEST_VERSION}.tar.gz \
    && tar -zxf googletest-v${GTEST_VERSION}.tar.gz \
    && rm googletest-v${GTEST_VERSION}.tar.gz

ENV ABSL_VERSION=20240116.1
RUN curl -Lo abseil-LTS${ABSL_VERSION}.tar.gz https://github.com/abseil/abseil-cpp/archive/refs/tags/${ABSL_VERSION}.tar.gz \
    && tar -zxf abseil-LTS${ABSL_VERSION}.tar.gz && cd abseil-cpp-${ABSL_VERSION} && mkdir build && cd build \
    && cmake -DCMAKE_CXX_STANDARD=14 -DCMAKE_PREFIX_PATH=/install -DCMAKE_INSTALL_PREFIX=/install \
    -DABSL_ENABLE_INSTALL=ON -DABSL_USE_EXTERNAL_GOOGLETEST=ON -DABSL_FIND_GOOGLETEST=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_TESTS=ON .. \
    && make -j$(nproc) \
    && ctest --test-dir . \
    && make install && cd

ENV S2_VERSION=0.11.1
ENV absl_DIR=/install/lib/cmake/absl
RUN curl -Lo s2geometry-v${S2_VERSION}.tar.gz https://github.com/google/s2geometry/archive/refs/tags/v${S2_VERSION}.tar.gz \
    && tar -zxf s2geometry-v${S2_VERSION}.tar.gz && cd s2geometry-${S2_VERSION} && mkdir build && cd build \
    && cmake -DCMAKE_PREFIX_PATH=/install/lib/cmake -DCMAKE_CXX_STANDARD=14 -DGOOGLETEST_ROOT=/usr/local/src/googletest-${GTEST_VERSION} \
    -DBUILD_SHARED_LIBS=ON -DWITH_PYTHON=ON -DGOOGLETEST_VERSION=${GTEST_VERSION} .. \
    && make -j$(nproc) \
    # && ctest --test-dir . \
    && make install

RUN python -m pip install cmake_build_extension wheel \
    && cd /usr/local/src/s2geometry-${S2_VERSION} \
    && sed -i.py "25 i                                        '-DGOOGLETEST_ROOT=/usr/local/src/googletest-${GTEST_VERSION}',\n                                        '-DGOOGLETEST_VERSION=${GTEST_VERSION}',\n" setup.py \
    && python setup.py bdist_wheel \
    && python -m pip install dist/*.whl

WORKDIR /root
COPY . ./indextool

RUN cd indextool && pip install -r requirements.txt
