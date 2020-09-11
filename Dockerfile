FROM debian:latest

# Download and install miniconda with wget
# RUN apt-get -qq update && apt-get -qq -y install wget
RUN apt-get update --fix-missing && apt-get install -y wget bzip2 ca-certificates libglib2.0-0 libxext6 libsm6 libxrender1 git mercurial subversion
RUN wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && bash /tmp/miniconda.sh -bp /opt/conda

# Set up conda
ENV PATH /opt/conda/bin:$PATH
RUN ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && echo "conda activate base" >> ~/.bashrc
RUN conda update --all

# Set up default environment as specified
COPY environment.yml /tmp/environment.yml
RUN conda config --set remote_read_timeout_secs 10000 && conda env update --prune -f /tmp/environment.yml

# Post cleanup
RUN rm -rf /tmp/miniconda.sh /tmp/environment.yml
# RUN apt-get -qq -y remove wget && apt-get -qq -y autoremove && apt-get autoclean && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log
