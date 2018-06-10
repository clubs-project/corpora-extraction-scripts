"""Runs all preprocessing scripts, assuming that clean_original_corpus.py and fast_elim_dupl_multi.py lie in the same directory
Requires Moses scripts: https://github.com/moses-smt/mosesdecoder/tree/master/scripts
"""
import clean_original_corpus
import fast_elim_dupl_multi
import os
import argparse
from time import time


class RunAll:
    def __init__(self, path_to_Moses_scripts, path_to_models, path_to_corpus, l1_code, l2_code, num_threads, tidy_up):
        self.moses = path_to_Moses_scripts
        self.models = path_to_models  # sth like models/modelTC.EpWP.en (la_code will be stripped automatically)
        self.corpus_path = path_to_corpus  # it suffices to give the path of one corpus, for retrieving, l1_code and l2_code will be used
        self.l1 = l1_code  # de, es, fr, en
        self.l2 = l2_code
        self.l1_path = self.corpus_path[:-2] + self.l1
        self.l2_path = self.corpus_path[:-2] + self.l2
        self.threads = num_threads
        self.tidy_up = tidy_up

    """Removes all sentences containing non-Latin characters and unescapes HTML character entities"""
    def fix_latin_html(self):
        cleaner = clean_original_corpus.Cleaner(self.l1_path, self.l2_path)
        return cleaner.run()

    def run_perl_scripts(self, file_path, la_code):
        stem = file_path[:-3]
        starting_time = time()
        print("Running normalization 1 for", la_code)
        norm1_path = stem + '.norm1.' + la_code
        norm1_cmd = 'perl ' + self.moses + 'tokenizer/replace-unicode-punctuation.perl < ' + file_path + ' > ' + norm1_path
        print("Command:", norm1_cmd)
        os.system(norm1_cmd)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        starting_time = time()
        print("Running normalization 2 for", la_code)
        norm2_path = stem + '.norm2.' + la_code
        norm2_cmd = 'perl ' + self.moses + 'tokenizer/normalize-punctuation.perl -l ' + la_code + ' < ' + norm1_path + ' > ' \
                    + norm2_path
        print("Command:", norm2_cmd)
        os.system(norm2_cmd)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        starting_time = time()
        print("Running normalization 3 for", la_code)
        norm3_path = stem + '.norm3.' + la_code
        norm3_cmd = 'perl ' + self.moses + 'tokenizer/remove-non-printing-char.perl < ' + norm2_path + ' > ' + norm3_path
        print("Command:", norm3_cmd)
        os.system(norm3_cmd)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        starting_time = time()
        print("Running tokenizer for", la_code)
        tok_path = stem + '.tok.' + la_code
        tok_cmd = 'perl ' + self.moses + 'tokenizer/tokenizer.perl -l ' + la_code + ' -no-escape -threads ' + \
                  str(self.threads) + ' < ' + norm3_path + ' > ' + tok_path
        print("Command:", tok_cmd)
        os.system(tok_cmd)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        starting_time = time()
        print("Running truecaser for", la_code)
        tc_path = stem + '.tc.' + la_code
        tc_cmd = 'perl ' + self.moses + 'recaser/truecase.perl --model ' + self.models[:-2] + la_code + ' < ' + tok_path\
                 + ' > ' + tc_path
        print("Command:", tc_cmd)
        os.system(tc_cmd)
        print("Done after {:.1f} seconds.".format(time() - starting_time))
        return tc_path

    def elim_dupl(self, l1_path, l2_path):
        eliminator = fast_elim_dupl_multi.FastElimDupl(l1_path, l2_path, self.threads)
        return eliminator.run()

    def tidy_up_folder(self, output_l1):
        corpus_folder = self.corpus_path[:(self.corpus_path.rfind('/')+1)]
        original_folder = corpus_folder + 'original/'  # path where all original files should be stored
        intermediate_folder = corpus_folder + 'intermediate/' # path where all intermediate results of the preprocessing are stored
        final_folder = corpus_folder + 'final/' # path where the final two corpora versions are stored
        os.system('mkdir ' + original_folder)
        os.system('mkdir ' + intermediate_folder)
        os.system('mkdir ' + final_folder)
        stem = self.corpus_path[:-2]
        os.system('mv ' + stem + self.l1 + ' ' + original_folder)
        os.system('mv ' + stem + self.l2 + ' ' + original_folder)
        os.system('mv ' + stem + 'ids' + ' ' + original_folder)
        os.system('mv ' + output_l1[:-2] + '* ' + final_folder)
        os.system('mv ' + stem + '* ' + intermediate_folder)

    def run(self):
        starting_time = time()
        new_l1_path, new_l2_path = self.fix_latin_html()
        l1_tc = self.run_perl_scripts(new_l1_path, self.l1)
        l2_tc = self.run_perl_scripts(new_l2_path, self.l2)
        output_l1, _ = self.elim_dupl(l1_tc, l2_tc)
        if self.tidy_up:
            self.tidy_up_folder(output_l1)
        print("Total running time: {:.1f} seconds".format(time() - starting_time))

if __name__ == "__main__":

    argparser = argparse.ArgumentParser(description="Run all preprocessing scripts for the corpora in Moses format")
    argparser.add_argument("moses", type=str, help="Path to Moses scripts, e.g. ../scripts/ (mandatory)")
    argparser.add_argument("models", type=str, help="Path to one of the models for the Moses truecaser, e.g. ../models/modelTC.EpWP.de (mandatory)")
    argparser.add_argument("corpus", type=str, help="Path to one of the corpus files (mandatory)")
    argparser.add_argument("l1_code", type=str, help="Code of L1, e.g. de (mandatory)")
    argparser.add_argument("l2_code", type=str, help="Code of L2, e.g. es (mandatory)")
    argparser.add_argument("-nt", "--num-threads", type=int, default=1,
                           help='Number of threads that can be used, default: 1')
    argparser.add_argument("-tu", "--tidy-up", default=False, action='store_true', help='Tidy up the folder with all the corpus files afterwards')
    args = argparser.parse_args()
    runner = RunAll(args.moses, args.models, args.corpus, args.l1_code, args.l2_code, args.num_threads, args.tidy_up)
    runner.run()

