# Removing false-positive datasets

Due to the indexation of zip files, we might have collected some false-positive datasets, 
i.e. datasets that contain zip files and matched our keywords but that eventually did not contain 
any molecular dynamics files (after looking into the zip files).

False-positive examples:

- Dataset [Genetic Variability among Complete Human Respiratory...](https://figshare.com/articles/dataset/Genetic_Variability_among_Complete_Human_Respiratory_Syncytial_Virus_Subgroup_A_Genomes_Bridging_Molecular_Evolutionary_Dynamics_and_Epidemiology__/116507) has probably been catched because of the words `Molecular` and `Dynamics` in the title but do not contain any MD data.
- Dataset [Homology Modeling of Dopamine](https://figshare.com/articles/dataset/Homology_Modeling_of_Dopamine_D_2_and_D_3_Receptors_Molecular_Dynamics_Refinement_and_Docking_Evaluation/120304) has been catched because of the expression `Molecular Dynamics` in the title and in the text. The dataset does not contain *real* MD files. Only PDB files.
- Dataset [Computational Study of HIV gp120 as a Target...](https://figshare.com/articles/dataset/Computational_Study_of_HIV_gp120_as_a_Target_for_Polyanionic_Entry_Inhibitors_Exploiting_the_V3_Loop_Region/14101790) contains snapshots from MD and Docking simulations.

Special cases:

- In dataset [CLN025__2RVD_3000ns_mutant](https://figshare.com/articles/dataset/CLN025_2RVD_3000ns_mutant/19794073), the archive `CLN025__2RVD_3000ns_mutant.zip` has no preview. Probably because the file is too large. In doubt, we keep datasets for which zip files cannot be previewed.
