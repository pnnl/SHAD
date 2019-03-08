FROM mminutoli/gentoo-mpich

LABEL mainteiner="Marco Minutoli <marco.minutoli@pnnl.gov>"

### INSTALL DEPENDENCIES
RUN emerge dev-vcs/git
RUN mkdir -p /etc/portage/repos.conf \
    && eselect repository list > /dev/null \
    && eselect repository add dev-cpp-overlay git https://github.com/mminutoli/dev-cpp-overlay.git \
    && emaint sync -r dev-cpp-overlay \
    && mkdir -p /etc/portage/package.accept_keywords \
    && echo "dev-libs/gmt ~amd64" >> /etc/portage/package.accept_keywords/gmt \
    && emerge app-doc/doxygen dev-python/pip dev-libs/gmt dev-cpp/gtest

USER mpi
RUN pip install --user sphinx breathe exhale sphinx_rtd_theme

USER root
