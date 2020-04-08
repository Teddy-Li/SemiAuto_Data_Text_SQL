from phrase_structures import *


class QRYNP:
	def __init__(self, np, typenps, propertynps, finalize_sequence=False):
		self.np = np
		self.z, self.z_toks, self.z_toks_novalue = self.process_z(typenps, propertynps)
		self.c_english_verbose = self.process_ce_verbose(typenps, propertynps)
		self.c_chinese_verbose = self.process_cc_verbose(typenps, propertynps)
		self.c_english_sequence = self.process_ce_step_by_step(typenps, propertynps, finalize=finalize_sequence)
		self.c_chinese_sequence = self.process_cc_step_by_step(typenps, propertynps, finalize=finalize_sequence)
		self.c_english = self.c_english_verbose
		self.c_chinese = self.c_chinese_verbose

	def process_z(self, typenps, propertynps):
		z = 'select '
		z_toks = ['select']

		if self.np.distinct:
			z += 'distinct '
			z_toks.append('distinct')

		# queried_props
		for idx, prop in enumerate(self.np.queried_props):
			if prop.dtype == 'star':
				z += prop.z.format('')
				z_toks += prop.z4toks.format('').split()
			elif len(self.np.table_ids) > 1:
				table_pos = None
				for i, item in enumerate(self.np.table_ids):
					if item == prop.table_id:
						table_pos = i+1
				z += prop.z.format('T{0}.'.format(table_pos))
				z_toks += prop.z4toks.format('T{0} . '.format(table_pos)).split()
			else:
				z += prop.z.format('')
				z_toks += prop.z4toks.format('').split()

			if idx < len(self.np.queried_props) - 1:
				z += ' , '
				z_toks.append(',')

		if len(self.np.queried_props) == 0:
			z += '*'
			z_toks.append('*')

		# table_ids
		z += ' from '
		z_toks.append('from')
		table_names = []
		temp_tabname_bucket = {}
		for no, tabid in enumerate(self.np.table_ids):
			if len(self.np.table_ids) == 1:
				table_names.append(typenps[tabid].z)
				temp_tabname_bucket[tabid] = ''
			else:
				table_names.append(typenps[tabid].z + ' as T%d ' % (no+1))
				temp_tabname_bucket[tabid] = 'T%d . ' % (no+1)

		z += (' ' + ' join '.join(table_names) + ' ')
		z_toks += (' ' + ' join '.join(table_names) + ' ').split(' ')

		# join_cdts
		if self.np.join_cdts is not None:
			if len(self.np.join_cdts) > 0:
				z += ' on '
				z_toks.append('on')
				join_cdt_zs = []
				for cond in self.np.join_cdts:
					join_cdt_zs.append(cond.fetch_z(temp_tabname_bucket))
				z += (' ' + ' and '.join(join_cdt_zs) + ' ')
				z_toks += (' ' + ' and '.join(join_cdt_zs) + ' ').split(' ')

		# where conditions
		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				z += ' where '
				z_toks.append('where')
				for idx, cond in enumerate(self.np.cdts):
					if idx > 0:
						z += self.np.cdt_linkers[idx - 1]
						z_toks.append(self.np.cdt_linkers[idx - 1])
					z += (' ' + cond.fetch_z(temp_tabname_bucket) + ' ')
					z_toks += cond.fetch_z(temp_tabname_bucket).split(' ')

		# group bys
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				z += ' group by '
				z_toks += ['group', 'by']
				groupby_propnames = []
				for idx, prop in enumerate(self.np.group_props):
					groupby_propnames.append(prop.z.format(temp_tabname_bucket[prop.table_id]))
				z += (' ' + ', '.join(groupby_propnames) + ' ')
				z_toks += (' ' + ', '.join(groupby_propnames) + ' ').split()

		# having conditions
		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			z += ' having '
			z_toks.append('having')
			for hv_idx, item in enumerate(self.np.having_cdts):
				z += item.fetch_z(temp_tabname_bucket)
				z_toks += item.fetch_z(temp_tabname_bucket).split(' ')
				if hv_idx < len(self.np.having_cdts)-1:
					z += ', '

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				z += ' order by '
				z_toks += ['order', 'by']
				orderby_propnames = []
				for idx, prop in enumerate(self.np.orderby_props):
					if prop.dtype == 'star':
						orderby_propnames.append(prop.z)
					else:
						orderby_propnames.append(prop.z.format(temp_tabname_bucket[prop.table_id]))
				z += (' ' + ', '.join(orderby_propnames) + ' ')
				z_toks += (' ' + ', '.join(orderby_propnames) + ' ').split()
				z += ' %s ' % self.np.orderby_order
				z_toks.append(self.np.orderby_order)

		if self.np.limit is not None:
			assert (self.np.limit > 0)
			z += 'limit %d' % self.np.limit
			z_toks += ['limit', '#%d#' % self.np.limit]  # use '#' to wrap the limit number as a 'value'

		if self.np.np_2 is not None:
			assert self.np.qrynp_2 is not None
			if self.np.has_union:
				z += ' union ' + self.np.qrynp_2.z
				z_toks.append('union')
				z_toks += self.np.qrynp_2.z_toks
			elif self.np.has_except:
				z += ' except ' + self.np.qrynp_2.z
				z_toks.append('except')
				z_toks += self.np.qrynp_2.z_toks
			elif self.np.has_intersect:
				z += ' intersect ' + self.np.qrynp_2.z
				z_toks.append('intersect')
				z_toks += self.np.qrynp_2.z_toks
			else:
				raise AssertionError

		# use '#' as an indicator for values
		z_toks_novalue = []
		z_toks_stripped = []
		for item in z_toks:
			if isinstance(item, list):
				raise AssertionError
			if '#' not in item:
				z_toks_novalue.append(item)
				z_toks_stripped.append(item)
			else:
				z_toks_novalue.append('value')
				if not (item[0] == '#' or item[-1] == '#'):
					print(z)
					raise AssertionError
				z_toks_stripped.append(item.strip('#'))
		z = ' '.join(z_toks_stripped)

		z_toks_fin = []
		z_toks_novalue_fin = []
		for item in z_toks_stripped:
			if len(item) > 0:
				z_toks_fin.append(item)
		for item in z_toks_novalue:
			if len(item) > 0:
				z_toks_novalue_fin.append(item)

		return z, z_toks_fin, z_toks_novalue_fin

	def process_ce_verbose(self, typenps, propertynps):
		c_english = ''
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_english += ' for each value of '
		else:
			self.np.group_props = []
		for idx, prop in enumerate(self.np.group_props):
			c_english += prop.c_english.format('')
			if idx + 2 < len(self.np.group_props):
				c_english += ', '
			elif idx + 2 == len(self.np.group_props):
				c_english += ' and '

		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			c_english += ' having '
			for hv_idx, item in enumerate(self.np.having_cdts):
				c_english += item.c_english
				if hv_idx < len(self.np.having_cdts)-1:
					c_english += ' , '
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_english += ' , '

		if self.np.distinct:
			c_english += 'all different values of '

		# queried_props
		tableid_of_last_prop = None
		for idx, prop in enumerate(self.np.queried_props):
			if isinstance(prop.table_id, list):
				c_english += prop.c_english
			else:
				if prop.table_id == tableid_of_last_prop:
					name = prop.c_english.format('')
				else:
					name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
					tableid_of_last_prop = prop.table_id
				c_english += name
			if idx + 2 < len(self.np.queried_props):
				c_english += ', '
			elif idx + 2 == len(self.np.queried_props):
				c_english += ' and '

		c_english += ' from '
		for idx, table_id in enumerate(self.np.table_ids):
			c_english += typenps[table_id].c_english
			if idx + 2 < len(self.np.table_ids):
				c_english += ', '
			elif idx + 2 == len(self.np.table_ids):
				c_english += ' and '

		if self.np.join_cdts is not None:
			if len(self.np.join_cdts) > 0:
				c_english += ' whose '
		else:
			self.np.join_cdts = []
		# all sorts of conditions
		for idx, cond in enumerate(self.np.join_cdts):
			c_english += cond.c_english
			if idx + 2 < len(self.np.join_cdts):
				c_english += ', '
			elif idx + 2 == len(self.np.join_cdts):
				c_english += ' and '

		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
					c_english += ','
				c_english += ' whose '
		else:
			self.np.cdts = []

		for idx, cond in enumerate(self.np.cdts):
			c_english += cond.c_english
			if idx + 1 < len(self.np.cdts):
				c_english += ' ' + self.np.cdt_linkers[idx] + ' '

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				assert self.np.orderby_order is not None
				if self.np.orderby_order == 'asc':
					_order = 'ascending'
					most = 'smallest'
				elif self.np.orderby_order == 'desc':
					_order = 'descending'
					most = 'largest'
				else:
					raise AssertionError
				if self.np.limit is None or self.np.limit == MAX_RETURN_ENTRIES:
					c_english += ', presented in %s order of ' % _order
				elif self.np.limit == 1:
					c_english += ' with the %s ' % most
				else:
					c_english += ', keep only %d of them with %s ' % (self.np.limit, most)
		else:
			self.np.orderby_props = []
		for idx, item in enumerate(self.np.orderby_props):
			c_english += item.c_english.format('')
			if idx + 1 < len(self.np.orderby_props):
				c_english += ', '

		if self.np.has_union:
			c_english = '( ' + c_english + ' ) combined with ( ' + self.np.qrynp_2.c_english + ' )'
		elif self.np.has_except:
			c_english = '( ' + c_english + ' ) except ( ' + self.np.qrynp_2.c_english + ' )'
		elif self.np.has_intersect:
			c_english = 'those in both ( ' + c_english + ' ) and ( ' + self.np.qrynp_2.c_english + ' )'

		return c_english

	def process_ce_step_by_step(self, typenps, propertynps, finalize=False):
		c_english = []
		sent_1 = 'Result {0[0]}: Find '
		if self.np.distinct:
			sent_1 += 'all different values of '

		num_tables = len(self.np.table_ids)

		# queried_props info
		tableid_of_last_prop = None
		need_from_even_single = False
		# if there are no group-by clauses in this query, write down the ultimate version of queried props directly
		if self.np.group_props is None or len(self.np.group_props) == 0:
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += prop.c_english
					if prop.aggr != 3:
						need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop:
						name = prop.c_english.format('')
					else:
						name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += ', '
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += ' and '
		else:
			# if there are group-by clauses in this query, at first we don't add use aggregators, instead we keep all entries
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += STAR_PROP.c_english
					need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop:
						name = propertynps[prop.overall_idx].c_english.format('')
					else:
						name = propertynps[prop.overall_idx].c_english.format(typenps[prop.table_id].c_english + '\'s ')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += ', '
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += ' and '

		if self.np.main_tid is None:
			self.np.main_tid = []

		# simplify the canonical utterances for join-on conditions when possible in order to help turkers understand
		# and more accurately annotate
		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tid) > 0:
			assert num_tables > 1 and len(self.np.table_ids) > 1
			for mid in self.np.main_tid:
				assert mid in self.np.table_ids
			sent_1 += ' from '
			for mtid_idx, mtid in enumerate(self.np.main_tid):
				sent_1 += typenps[mtid].c_english
				if mtid_idx < len(self.np.main_tid)-1:
					sent_1 += ', '
			sent_1 += ' and their corresponding '
			other_tids = copy.deepcopy(self.np.table_ids)
			for mtid in self.np.main_tid:
				other_tids.remove(mtid)
			for mtid in self.np.main_tid:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				sent_1 += typenps[table_id].c_english
				if idx + 2 < len(other_tids):
					sent_1 += ', '
				elif idx + 2 == len(other_tids):
					sent_1 += ' and '
		else:
			# table info
			if num_tables > 1 or need_from_even_single:
				sent_1 += ' from '
				for idx, table_id in enumerate(self.np.table_ids):
					sent_1 += typenps[table_id].c_english
					if idx + 2 < len(self.np.table_ids):
						sent_1 += ', '
					elif idx + 2 == len(self.np.table_ids):
						sent_1 += ' and '
			# join-on conditions info
			if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
				sent_1 += ' whose '
			else:
				self.np.join_cdts = []
			for idx, cond in enumerate(self.np.join_cdts):
				sent_1 += cond.c_english
				if idx + 2 < len(self.np.join_cdts):
					sent_1 += ', '
				elif idx + 2 == len(self.np.join_cdts):
					sent_1 += ' and '

		c_english.append(sent_1)

		# where conditions info
		is_and = None  # whether the condition linker for where conditions is 'and'
		if self.np.cdts is None:
			self.np.cdts = []
		_idx = 0
		while _idx < len(self.np.cdts):
			if _idx + 1 < len(self.np.cdts):
				next_column_id = self.np.cdts[_idx + 1].left.overall_idx
			else:
				next_column_id = None
			# allows for 'and' & 'or' linkers both present in a set of 'where' conditions (though not useful as of
			# SPIDER's scope)
			if is_and is None:
				cur_sent = 'Result {0[%d]}: From Result {0[%d]}, find those satisfying ' % (
					len(c_english), len(c_english) - 1)
			else:
				if is_and:
					cur_sent = 'Result {0[%d]}: From Result {0[%d]}, find those satisfying ' % (
						len(c_english), len(c_english) - 1)
				else:
					cur_sent = 'Result {0[%d]}: Find in addition to Result {0[%d]}, those from Result {0[%d]} satisfying ' % (
						len(c_english), len(c_english) - 1, max(0, len(c_english)-2))
			cur_sent += self.np.cdts[_idx].c_english
			if next_column_id is not None and next_column_id == self.np.cdts[_idx].left.overall_idx:
				cur_sent += ' ' + self.np.cdt_linkers[_idx]
				next_cond = self.np.cdts[_idx + 1]
				if isinstance(next_cond.right, numpy.ndarray) or isinstance(next_cond.right, list):
					value_ce = []
					for item in next_cond.right:
						value_ce.append(item.c_english)
					utt_next = ' ' + next_cond.cmper.c_english.format(value_ce)
				elif isinstance(next_cond.right, PROPERTYNP):
					utt_next = ' ' + next_cond.cmper.c_english.format(next_cond.right.c_english.format(''))
				elif isinstance(next_cond.right, QRYNP):
					utt_next = ' ' + next_cond.cmper.c_english.format(' ( ' + next_cond.right.c_english + ' ) ')
				else:
					raise AssertionError
				if 'is' in utt_next[:4]:
					utt_next = utt_next[4:]
				cur_sent += utt_next
				_idx += 1
			if _idx < len(self.np.cdts) - 1:
				is_and = (self.np.cdt_linkers[_idx] == 'and')  # is_and value may change from True to False
			c_english.append(cur_sent)
			_idx += 1

		selected_in_groupby = True
		for prop_1 in self.np.queried_props:
			in_groupby = False
			for prop_2 in self.np.group_props:
				if prop_1.overall_idx == prop_2.overall_idx:
					in_groupby = True
					break
			if not in_groupby:
				selected_in_groupby = False
				break

		# groupby info
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				groupby_sent = 'Result {0[%d]}: From Result {0[%d]}, for each value of ' % (
					len(c_english), len(c_english) - 1)
				for idx, prop in enumerate(self.np.group_props):
					groupby_sent += prop.c_english.format('')
					if idx + 2 < len(self.np.group_props):
						groupby_sent += ', '
					elif idx + 2 == len(self.np.group_props):
						groupby_sent += ' and '
				if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
					groupby_sent += ' where '
					for item in self.np.having_cdts:
						groupby_sent += (item.c_english + ' , ')
				else:
					groupby_sent += ' , '

				# if there are group-by clauses in query, specify the aggregators of queried props along with the
				# group-by clause itself
				groupby_sent += 'find '

				if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
					groupby_sent += 'top %d of ' % self.np.limit

				if self.np.distinct:
					if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
						groupby_sent += 'different values of '
					else:
						groupby_sent += 'all different values of '
				tableid_of_last_prop = None
				for idx, prop in enumerate(self.np.queried_props):
					# if it is a '*' column
					if isinstance(prop.table_id, list):
						groupby_sent += prop.c_english
					else:
						if prop.table_id == tableid_of_last_prop:
							name = prop.c_english.format('')
						else:
							name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
							tableid_of_last_prop = prop.table_id
						groupby_sent += name
					if idx + 2 < len(self.np.queried_props):
						groupby_sent += ', '
					elif idx + 2 == len(self.np.queried_props):
						groupby_sent += ' and '

				if selected_in_groupby and self.np.orderby_props is not None and len(self.np.orderby_props) > 0:
					if self.np.orderby_order == 'asc':
						_order = 'ascending'
					elif self.np.orderby_order == 'desc':
						_order = 'descending'
					else:
						raise AssertionError
					groupby_sent += ' in %s order of ' % _order
					for idx, item in enumerate(self.np.orderby_props):
						groupby_sent += item.c_english.format('')
						if idx + 1 < len(self.np.orderby_props):
							groupby_sent += ' then '
				c_english.append(groupby_sent)

		if selected_in_groupby:
			assert self.np.group_props is not None

		if self.np.orderby_props is not None and not selected_in_groupby:
			if len(self.np.orderby_props) > 0:
				assert self.np.orderby_order is not None
				istop1 = None
				if self.np.limit is not None and self.np.limit == 1:
					istop1 = True
					if self.np.orderby_order == 'asc':
						_order = 'least'
					elif self.np.orderby_order == 'desc':
						_order = 'most'
					else:
						raise AssertionError
					orderby_sent = 'Result {0[%d]}: find Result {0[%d]} with the %s ' % (
					len(c_english), len(c_english) - 1, _order)
				else:
					istop1 = False
					if self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
						limit_ce = 'top %d of ' % self.np.limit
					else:
						limit_ce = ''
					if self.np.orderby_order == 'asc':
						_order = 'ascending'
					elif self.np.orderby_order == 'desc':
						_order = 'descending'
					else:
						raise AssertionError
					orderby_sent = 'Result {0[%d]}: present %sResult {0[%d]} in %s order of ' % (
						len(c_english), limit_ce, len(c_english) - 1, _order)
				for idx, item in enumerate(self.np.orderby_props):
					orderby_sent += item.c_english.format('')
					if idx + 1 < len(self.np.orderby_props):
						if istop1:
							if idx+2 == len(self.np.orderby_props):
								orderby_sent += ' and '
							else:
								orderby_sent += ', '
						else:
							orderby_sent += ' then '
				c_english.append(orderby_sent)

		if self.np.has_union or self.np.has_except or self.np.has_intersect:
			second_sents = self.np.qrynp_2.c_english_sequence
			len_cur_sents = len(c_english)
			len_second_sents = len(second_sents)
			second_sents = ' ### '.join(second_sents)
			second_sents = second_sents.format([('{0[%d]}' % (i + len(c_english))) for i in range(len_second_sents)])
			second_sents = second_sents.split(' ### ')
			c_english += second_sents
			if self.np.has_union:
				post_clause = 'Result {0[%d]}: return those in Result {0[%d]} as well as those in Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			elif self.np.has_except:
				post_clause = 'Result {0[%d]}: return those in Result {0[%d]} except those in Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			elif self.np.has_intersect:
				post_clause = 'Result {0[%d]}: return those in both Result {0[%d]} and Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			else:
				raise AssertionError
			c_english.append(post_clause)

		# in case of the upper most layer of recursion, finalize the sequence by replacing the '{0[xxx]}' number
		# placeholders with actual numbers 'xxx'
		if finalize:
			total_len = len(c_english)
			c_english = ' ### '.join(c_english)
			c_english = c_english.format([i for i in range(total_len)])
			c_english = c_english.split(' ### ')

		return c_english

	def process_cc_verbose(self, typenps, propertynps):
		c_chinese = ''
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_chinese += '对于每一个'
		else:
			self.np.group_props = []

		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			c_chinese += '满足'
			for hv_idx, item in enumerate(self.np.having_cdts):
				c_chinese += item.c_chinese
				if hv_idx < len(self.np.having_cdts) - 1:
					c_chinese += '、'
			c_chinese += '的'

		for idx, prop in enumerate(self.np.group_props):
			c_chinese += prop.c_chinese.format('')
			if idx + 2 < len(self.np.group_props):
				c_chinese += '、'
			elif idx + 2 == len(self.np.group_props):
				c_chinese += '和'
		if len(self.np.group_props) > 0:
			c_chinese += '，'

		c_chinese += '从'
		for idx, table_id in enumerate(self.np.table_ids):
			c_chinese += typenps[table_id].c_chinese
			if idx + 2 < len(self.np.table_ids):
				c_chinese += '、'
			elif idx + 2 == len(self.np.table_ids):
				c_chinese += '和'
		c_chinese += '中找出，'

		if self.np.join_cdts is not None:
			if len(self.np.join_cdts) > 0:
				c_chinese += '满足'
		else:
			self.np.join_cdts = []
		# all sorts of conditions
		for idx, cond in enumerate(self.np.join_cdts):
			c_chinese += cond.c_chinese
			if idx + 2 <= len(self.np.join_cdts):
				c_chinese += '、'
		if len(self.np.join_cdts) > 0:
			c_chinese += '的，'

		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				c_chinese += '满足'
		else:
			self.np.cdts = []
		for idx, cond in enumerate(self.np.cdts):
			c_chinese += cond.c_chinese
			if idx + 1 < len(self.np.cdts):
				if self.np.cdt_linkers[idx] == 'or':
					c_chinese += '或'
				elif self.np.cdt_linkers[idx] == 'and':
					c_chinese += '且'
				else:
					raise AssertionError
		if len(self.np.cdts) > 0:
			c_chinese += '的，'

		# queried_props
		tableid_of_last_prop = None
		for idx, prop in enumerate(self.np.queried_props):
			if isinstance(prop.table_id, list):
				c_chinese += prop.c_chinese
			else:
				if prop.table_id == tableid_of_last_prop:
					name = prop.c_chinese.format('')
				else:
					name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
					tableid_of_last_prop = prop.table_id
				c_chinese += name
			if idx + 2 < len(self.np.queried_props):
				c_chinese += '、'
			elif idx + 2 == len(self.np.queried_props):
				c_chinese += '和'

		if self.np.distinct:
			c_chinese += '的全部不同取值'

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				orderby_props_cc = ''
				for idx, item in enumerate(self.np.orderby_props):
					orderby_props_cc += item.c_chinese.format('')
					if idx + 1 < len(self.np.orderby_props):
						orderby_props_cc += '、'
				assert self.np.orderby_order is not None
				if self.np.orderby_order == 'asc':
					_order = '升'
				elif self.np.orderby_order == 'desc':
					_order = '降'
				else:
					raise AssertionError
				if self.np.limit is None or self.np.limit == MAX_RETURN_ENTRIES:
					c_chinese += '，按%s的%s序排列' % (orderby_props_cc, _order)
				else:
					if self.np.orderby_order == 'asc':
						most = '低'
					elif self.np.orderby_order == 'desc':
						most = '高'
					else:
						raise AssertionError
					c_chinese += '，仅选出%s最%s的%d个' % (orderby_props_cc, most, self.np.limit)
		else:
			self.np.orderby_props = []

		if self.np.has_union:
			c_chinese = '返回（' + c_chinese + '）中的全部内容和（' + self.np.qrynp_2.c_chinese + '）中的全部内容'
		elif self.np.has_except:
			c_chinese = '返回（' + c_chinese + '）的内容中，除去（' + self.np.qrynp_2.c_chinese + '）之外的部分'
		elif self.np.has_intersect:
			c_chinese = '返回同时在（' + c_chinese + '）和（' + self.np.qrynp_2.c_chinese + '）中的内容'
		return c_chinese

	def process_cc_step_by_step(self, typenps, propertynps, finalize=False):
		c_chinese = []
		sent_1 = 'Result {0[0]}：'
		num_tables = len(self.np.table_ids)

		need_from_even_single = False
		for idx, prop in enumerate(self.np.queried_props):
			# if it is a '*' column
			if isinstance(prop.table_id, list):
				sent_1 += prop.c_chinese
				if prop.aggr != 3:
					need_from_even_single = True

		# simplify the canonical utterances for join-on conditions when possible in order to help turkers understand
		# and more accurately annotate
		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tid) > 0:
			assert num_tables > 1 and len(self.np.table_ids) > 1
			for mid in self.np.main_tid:
				assert mid in self.np.table_ids
			sent_1 += '从'
			for mtid_idx, mtid in enumerate(self.np.main_tid):
				sent_1 += typenps[mtid].c_chinese
				if mtid_idx < len(self.np.main_tid)-1:
					sent_1 += '、'
			sent_1 += '以及相应的'
			other_tids = self.np.table_ids
			for mtid in self.np.main_tid:
				other_tids.remove(mtid)
			for mtid in self.np.main_tid:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				sent_1 += typenps[table_id].c_chinese
				if idx + 2 < len(other_tids):
					sent_1 += '、'
				elif idx + 2 == len(other_tids):
					sent_1 += '和'
			sent_1 += '中，找出'
		else:
			# table info
			if num_tables > 1 or need_from_even_single:
				sent_1 += '从'
				# join-on conditions info
				if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
					sent_1 += '符合'
				else:
					self.np.join_cdts = []

				for idx, cond in enumerate(self.np.join_cdts):
					sent_1 += cond.c_chinese
					if idx + 2 < len(self.np.join_cdts):
						sent_1 += '、'
					elif idx + 2 == len(self.np.join_cdts):
						sent_1 += '以及'
				if len(self.np.join_cdts) > 0:
					sent_1 += '的'
				for idx, table_id in enumerate(self.np.table_ids):
					sent_1 += typenps[table_id].c_chinese
					if idx + 2 < len(self.np.table_ids):
						sent_1 += '、'
					elif idx + 2 == len(self.np.table_ids):
						sent_1 += '和'
				sent_1 += '中，找出'
			else:
				sent_1 += '找出'

		if self.np.distinct:
			sent_1 += '全部不同的'

		# queried_props info
		tableid_of_last_prop = None
		# if there are no group-by clauses in this query, write down the ultimate version of queried props directly
		if self.np.group_props is None or len(self.np.group_props) == 0:
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += prop.c_chinese
				else:
					if prop.table_id == tableid_of_last_prop:
						name = prop.c_chinese.format('')
					else:
						name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += '、'
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += '和'
		else:
			# if there are group-by clauses in this query, at first we don't add use aggregators, instead we keep all entries
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += STAR_PROP.c_chinese
					need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop:
						name = propertynps[prop.overall_idx].c_chinese.format('')
					else:
						name = propertynps[prop.overall_idx].c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += '、'
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += '和'
		c_chinese.append(sent_1)

		# where conditions inf
		is_and = None  # whether the condition linker for where conditions is 'and'
		if self.np.cdts is None:
			self.np.cdts = []
		linkers_trans = {'and': '且', 'or': '或'}  # translates cdt_linkers 'and' & 'or' into Chinese words
		_idx = 0  # the index of condition to work on
		while _idx < len(self.np.cdts):
			if _idx + 1 < len(self.np.cdts):
				next_column_id = self.np.cdts[_idx + 1].left.overall_idx  # if not the last one, check the column for next cdt
			else:
				next_column_id = None
			# allows for 'and' & 'or' linkers both present in a set of 'where' conditions (though not useful as of
			# SPIDER's scope)
			if is_and is None:
				cur_sent = 'Result {0[%d]}：从 Result {0[%d]}中，找出满足' % (
					len(c_chinese), len(c_chinese) - 1)
			else:
				if is_and:
					cur_sent = 'Result {0[%d]}：从Result {0[%d]}中，找出满足' % (
						len(c_chinese), len(c_chinese) - 1)
				else:
					cur_sent = 'Result {0[%d]}: 在Result {0[%d]}之余，找出 Result {0[%d]}中满足' % (
						len(c_chinese), len(c_chinese) - 1, max(0, len(c_chinese)-2))
			cur_sent += self.np.cdts[_idx].c_chinese
			if next_column_id is not None and next_column_id == self.np.cdts[_idx].left.overall_idx:
				cur_sent += linkers_trans[self.np.cdt_linkers[_idx]]
				next_cond = self.np.cdts[_idx + 1]
				if isinstance(next_cond.right, numpy.ndarray) or isinstance(next_cond.right, list):
					value_cc = []
					for item in next_cond.right:
						value_cc.append(item.c_chinese)
					utt_next = next_cond.cmper.c_chinese.format(value_cc)
				elif isinstance(next_cond.right, PROPERTYNP):
					utt_next = next_cond.cmper.c_chinese.format(next_cond.right.c_chinese.format(''))
				elif isinstance(next_cond.right, QRYNP):
					utt_next = next_cond.cmper.c_chinese.format(' ( ' + next_cond.right.c_chinese + ' ) ')
				else:
					raise AssertionError
				cur_sent += utt_next
				_idx += 1
			cur_sent += '的部分'
			if _idx < len(self.np.cdts) - 1:
				is_and = (self.np.cdt_linkers[_idx] == 'and')  # is_and value may change from True to False
			c_chinese.append(cur_sent)
			_idx += 1


		# check if every selected property can be found in 'group-by' property list
		selected_in_groupby = True
		for prop_1 in self.np.queried_props:
			in_groupby = False
			for prop_2 in self.np.group_props:
				if prop_1.overall_idx == prop_2.overall_idx:
					in_groupby = True
					break
			if not in_groupby:
				selected_in_groupby = False
				break

		# groupby info
		if self.np.group_props is not None and len(self.np.group_props) > 0:
			groupby_sent = 'Result {0[%d]}：从Result {0[%d]}中，在每组' % (
				len(c_chinese), len(c_chinese) - 1)

			if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
				groupby_sent += '满足'
				for hv_idx, item in enumerate(self.np.having_cdts):
					groupby_sent += item.c_chinese
					if hv_idx+1 < len(self.np.having_cdts):
						groupby_sent += '、'
				groupby_sent += '的'

			for idx, prop in enumerate(self.np.group_props):
				groupby_sent += prop.c_chinese.format('')
				if idx + 2 < len(self.np.group_props):
					groupby_sent += '、'
				elif idx + 2 == len(self.np.group_props):
					groupby_sent += '和'


			# if there are group-by clauses in query, specify the aggregators of queried props along with the
			# group-by clause itself
			groupby_sent += '下，'

			if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
				groupby_sent += '找出具有'
				if self.np.orderby_order == 'asc':
					groupby_sent += '最小'
				elif self.np.orderby_order == 'desc':
					groupby_sent += '最大'
				else:
					raise AssertionError
				for ob_idx, item in enumerate(self.np.orderby_props):
					groupby_sent += item.c_chinese.format('')
					if ob_idx+1 < len(self.np.orderby_props):
						groupby_sent += '、'
				groupby_sent += '的'
				if self.np.limit != 1:
					groupby_sent += '前%d个' % self.np.limit

			elif selected_in_groupby and self.np.orderby_props is not None and len(self.np.orderby_props) > 0:
				groupby_sent += '按照'
				for ob_idx, item in enumerate(self.np.orderby_props):
					groupby_sent += item.c_chinese.format('')
					if ob_idx+1 < len(self.np.orderby_props):
						groupby_sent += '、'
				groupby_sent += '的'
				if self.np.orderby_order == 'asc':
					groupby_sent += '升序'
				elif self.np.orderby_order == 'desc':
					groupby_sent += '降序'
				else:
					raise AssertionError
				groupby_sent += '找出'

			tableid_of_last_prop = None
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					groupby_sent += prop.c_chinese
				else:
					if prop.table_id == tableid_of_last_prop:
						name = prop.c_chinese.format('')
					else:
						name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					groupby_sent += name
				if idx + 2 < len(self.np.queried_props):
					groupby_sent += '、'
				elif idx + 2 == len(self.np.queried_props):
					groupby_sent += '和'

			if self.np.distinct:
				groupby_sent += '，找出其所有的不同取值'
			c_chinese.append(groupby_sent)

		if selected_in_groupby:
			assert self.np.group_props is not None

		if self.np.orderby_props is not None and len(self.np.orderby_props) > 0 and not selected_in_groupby:
			assert self.np.orderby_order is not None
			if self.np.limit is not None and self.np.limit == 1:
				istop1 = True
				if self.np.orderby_order == 'asc':
					_order = '最小的'
				elif self.np.orderby_order == 'desc':
					_order = '最大的'
				else:
					raise AssertionError
				orderby_sent = 'Result {0[%d]}：找出 Result {0[%d]}中' % (
				len(c_chinese), len(c_chinese) - 1)

			else:
				istop1 = False
				if self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
					limit_cc = '选出前%d个条目' % self.np.limit
				else:
					limit_cc = '排列'
				if self.np.orderby_order == 'asc':
					_order = '升序'
				elif self.np.orderby_order == 'desc':
					_order = '降序'
				else:
					raise AssertionError
				orderby_sent = 'Result {0[%d]}: 将Result {0[%d]}按' % (
					len(c_chinese), len(c_chinese) - 1)

			for idx, item in enumerate(self.np.orderby_props):
				orderby_sent += item.c_chinese.format('')
				if idx + 1 < len(self.np.orderby_props):
					orderby_sent += '、'

			if istop1:
				orderby_sent += _order
			else:
				orderby_sent += '的%s%s' % (_order, limit_cc)
			c_chinese.append(orderby_sent)

		if self.np.has_union or self.np.has_except or self.np.has_intersect:
			second_sents = self.np.qrynp_2.c_chinese_sequence
			len_cur_sents = len(c_chinese)
			len_second_sents = len(second_sents)
			second_sents = ' ### '.join(second_sents)
			second_sents = second_sents.format([('{0[%d]}' % (i + len(c_chinese))) for i in range(len_second_sents)])
			second_sents = second_sents.split(' ### ')
			c_chinese += second_sents
			if self.np.has_union:
				post_clause = 'Result {0[%d]}：返回 Result {0[%d]} 中的全部内容和 Result {0[%d]} 中的全部内容' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
				assert not self.np.has_except and not self.np.has_intersect
			elif self.np.has_except:
				post_clause = 'Result {0[%d]}：返回 Result {0[%d]} 中不被 Result {0[%d]} 包含的部分' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
				assert not self.np.has_intersect
			elif self.np.has_intersect:
				post_clause = 'Result {0[%d]}：返回所有同时在 Result {0[%d]} 和 Result {0[%d]} 中的部分' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			else:
				raise AssertionError
			c_chinese.append(post_clause)

		# in case of the upper most layer of recursion, finalize the sequence by replacing the '{0[xxx]}' number
		# placeholders with actual numbers 'xxx'
		if finalize:
			total_len = len(c_chinese)
			c_chinese = ' ### '.join(c_chinese)
			c_chinese = c_chinese.format([i for i in range(total_len)])
			c_chinese = c_chinese.split(' ### ')

		return c_chinese