#!/bin/bash
#
#SBATCH -o ./slurmresults/slurm-%j.out # STDOUT
#SBATCH --error ./slurm_error_results/slurm-%j.out # STDOUT
#SBATCH --job-name=NGSPipe
#SBATCH --mail-type=END
#SBATCH --mail-user=zachptom@gmail.com
#
#SBATCH --cpus-per-task=8

module load sratoolkit fastqc bwa samtools vcftools-0.1.13

genome=~/pipeline/hg38/hg38.fa

#Uncomment the line below for your first time running the pipeline only
#bwa index $genome
mkdir -p sam bam bcf vcf artifacts artifacts/all

echo "Downloading $1 ..."
prefetch $1
echo "Running Fastq-dump ..."
fastq-dump --split-files -gzip $1 --outdir ~/pipeline/fastq
#cd ~/ncbi/public/sra
#rm "$1".sra
cd ~/pipeline/fastq
echo "Running FastQC ..."
fastqc "$1"_1.fastq.gz "$1"_2.fastq.gz --outdir=../fastqc
echo "Running Trimmomatic ..."
java -jar ~/pipeline/trimmomatic-0.39.jar PE ~/pipeline/fastq/"$1"_1.fastq.gz ~/pipeline/fastq/"$1"_2.fastq.gz -baseout ~/pipeline/trimmomatic_output/"$1".fq ILLUMINACLIP:/home/ztom/pipeline/fasta/TruSeq3-PE.fa:2:30:10
echo "Running bwa ..."

fq1=~/pipeline/trimmomatic_output/"$1"_1P.fq
fq2=~/pipeline/trimmomatic_output/"$1"_2P.fq
sam=~/pipeline/sam/"$1".aligned.sam 
bam=~/pipeline/bam/"$1".aligned.bam
sorted_bam=~/pipeline/bam/"$1".aligned.sorted.bam
raw_bcf=~/pipeline/bcf/"$1"_raw.bcf
variants=~/pipeline/bcf/"$1"_variants.vcf
final_variants=~/pipeline/vcf/"$1"_final_variants.vcf
filtered_variants=~/pipeline/vcf/"$1"_filtered_variants.vcf.gz
artifacts=~/pipeline/artifacts/"$1" 
artifacts_tab="$artifacts"/"$1"_artifacts.tab

bwa mem -t 8 $genome $fq1 $fq2 > $sam
echo "Running samtools ..."
samtools view -S -b $sam > $bam
samtools sort -o $sorted_bam $bam
samtools index $sorted_bam
echo "Running bcftools ..."
bcftools mpileup -O b -o $raw_bcf -f $genome $sorted_bam
bcftools call --ploidy 1 -m -v -o $variants $raw_bcf
echo "Running vcfutils.pl ..." 
vcfutils.pl varFilter $variants > $final_variants

echo "Filtering variants against high confidence calls ..."
cd ~/pipeline
bedtools intersect -a $final_variants -b NA12878_GIAB.vcf -header | bgzip -c > $filtered_variants
bcftools index $filtered_variants
echo "Finding potential sequencing artifacts ..."
mkdir $artifacts
bcftools isec -p $artifacts -Ov -C $filtered_variants NA12878_GIAB.vcf.gz

echo "Converting vcf file to tab-delimited format for ML analysis ..."
#the following line only needs to be executed once after initial installation of vcftools
export PERL5LIB=/opt/vcftools_0.1.13/perl
cat $artifacts/0000.vcf | vcf-to-tab > $artifacts_tab
echo "Correcting column names of .tab file ..."
cn=$(awk 'NR==1 {print $4}' $artifacts_tab)
sed "1s|${cn}|ALT|" $artifacts_tab > "$artifacts_tab".tmp && mv "$artifacts_tab".tmp $artifacts_tab
cp $artifacts_tab ~/pipeline/artifacts/all
