FROM ubuntu:focal

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install -y openssh-server sudo build-essential libopenmpi-dev git wget

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
RUN echo 'deb https://apt.kitware.com/ubuntu/ focal main' > /etc/apt/sources.list.d/kitware.list && \
    apt-get update -y && apt-get install -y cmake libgoogle-perftools-dev libgtest-dev

RUN mkdir /var/run/sshd
RUN echo 'root:tutorial' | chpasswd
RUN echo 'PermitRootLogin yes' > /etc/ssh/sshd_config.d/root_login.conf

# Add a user for the tutorial
RUN adduser --disabled-password --gecos "" tutorial && \
    echo "tutorial ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
ENV HOME /home/tutorial

# Setup Keys
RUN mkdir /home/tutorial/.ssh
ADD ssh/config /home/tutorial/.ssh/config
ADD ssh/id_rsa.mpi /home/tutorial/.ssh/id_rsa
ADD ssh/id_rsa.mpi.pub /home/tutorial/.ssh/id_rsa.pub
ADD ssh/id_rsa.mpi.pub /home/tutorial/.ssh/authorized_keys


# Install GMT
WORKDIR /home/tutorial

RUN git clone https://github.com/pnnl/gmt.git
RUN cmake -S gmt -B gmt-build && cmake --build gmt-build && cmake --install gmt-build
RUN git clone -b ics_2022_tutorial https://github.com/pnnl/SHAD.git
RUN cmake -S SHAD -B SHAD-build -DCMAKE_BUILD_TYPE=Release -DSHAD_RUNTIME_SYSTEM=GMT && \
    cmake --build SHAD-build && \
    cmake --install SHAD-build

RUN chmod -R 600 /home/tutorial/.ssh/* && \
    chown -R tutorial:tutorial /home/tutorial/.ssh

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
